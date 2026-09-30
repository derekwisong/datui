use color_eyre::Result;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use polars::datatypes::AnyValue;
use polars::datatypes::DataType;
#[cfg(feature = "cloud")]
use polars::io::cloud::{AmazonS3ConfigKey, CloudOptions};
use polars::prelude::{DataFrame, LazyFrame, Schema, col};
#[cfg(feature = "cloud")]
use polars::prelude::{PlRefPath, ScanArgsParquet};
use std::collections::HashMap;

/// Rows measured per background pass. Small enough that a slow filesystem shows
/// progress rather than a long silence.
const MEASURE_BATCH: usize = 12;

/// Rows a probe measures while it is already reading a remote directory.
const PROBE_MEASURE_LIMIT: usize = 24;

/// Rows one classification pass looks into.
///
/// A cap on work in flight rather than a budget spent per directory: what gets looked
/// into is what is on screen, and the next pass is chosen from the viewport as it is
/// when the previous one lands. Sized like [`MEASURE_BATCH`], for the same reason —
/// on a share that answers in milliseconds per row, a screenful arriving in pieces
/// reads as filling in, and one long silence reads as broken.
const CLASSIFY_BATCH: usize = 16;

/// Probes allowed at once. A probe of a share that has gone away holds its thread
/// until the process exits, so the number of them has to be bounded.
const MAX_CONCURRENT_PROBES: usize = 4;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc::Sender};
use widgets::info::{InfoFocus, InfoModal, InfoTab, ParquetMetadataCache, read_parquet_metadata};

use ratatui::style::{Color, Style};
use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};

use ratatui::widgets::{Block, Clear};

pub mod analysis_modal;
#[cfg(feature = "cloud")]
pub mod aws_profiles;
#[cfg(feature = "cloud")]
pub mod azure;
pub mod cache;
pub mod canonical;
pub mod chart_data;
pub mod chart_export;
pub mod chart_export_modal;
pub mod chart_modal;
pub mod cli;
pub mod clipboard;
#[cfg(feature = "cloud")]
pub mod cloud_browse;
#[cfg(feature = "cloud")]
pub mod cloud_command;
pub mod cloud_env;
#[cfg(feature = "cloud")]
mod cloud_hive;
#[cfg(feature = "cloud")]
pub mod cloud_sources;
pub mod config;
pub mod copy_modal;
pub mod data_quality;
pub mod discover;
pub mod distribution_fit;
pub mod error_display;
pub mod event_pump;
pub mod export_modal;
pub mod filter_modal;
pub mod fuzzy;
#[cfg(feature = "cloud")]
pub mod gcloud;
pub mod glyphs;
pub(crate) mod help_strings;
pub mod home;
pub mod locality;
pub mod measurements;
pub mod notes;
pub mod numfmt;
pub mod pivot_melt_modal;
pub mod quality_report;
#[cfg(feature = "cloud")]
pub mod s3_tools;
pub mod sample_modal;
pub mod sampling;
// Public so the fuzz targets in `fuzz/` can reach `parse_query`. The parser is
// hand-written and runs on whatever the user types, so it is fuzzed directly.
pub mod query;
mod render;
pub mod sanitize;
pub mod schema_union;
pub mod search;
pub mod sort_filter_modal;
pub mod sort_modal;
pub mod source;
pub mod statistics;
pub mod template;
pub mod widgets;

pub use cache::CacheManager;
pub use cli::Args;
pub use config::{
    AppConfig, ColorParser, ConfigManager, QueryMode, Theme, rgb_to_256_color, rgb_to_basic_ansi,
};

use analysis_modal::{AnalysisModal, AnalysisProgress};
use chart_export::{
    BoxPlotExportBounds, ChartExportBounds, ChartExportFormat, ChartExportSeries,
    write_box_plot_eps, write_box_plot_png, write_chart_eps, write_chart_png, write_heatmap_eps,
    write_heatmap_png,
};
use chart_export_modal::{ChartExportFocus, ChartExportModal};
use chart_modal::{ChartFocus, ChartKind, ChartModal, ChartType};
pub use error_display::{ErrorKindForPython, error_for_python};
use export_modal::{ExportFocus, ExportFormat, ExportModal};
use filter_modal::{FilterEditStep, FilterStatement};
use numfmt::NumberFormatSettings;
use pivot_melt_modal::{MeltSpec, PivotMeltFocus, PivotMeltModal, PivotMeltTab, PivotSpec};
use sort_filter_modal::{SortFilterFocus, SortFilterModal, SortFilterTab};
use sort_modal::{SortColumn, SortFocus};
pub use template::{Template, TemplateManager};
use widgets::controls::Controls;
use widgets::datatable::{DataTableState, DrillRow};
use widgets::debug::DebugState;
use widgets::template_modal::{FormFocus, TemplateModal, TemplateModalMode, ViewRow};
use widgets::text_input::{TextInput, TextInputEvent};

/// Application name used for cache directory and other app-specific paths
pub const APP_NAME: &str = "datui";

/// Re-export compression format and file format from CLI module
pub use cli::{CompressionFormat, FileFormat};

/// Map FileFormat to ExportFormat for default export. Tsv/Psv map to Csv; Orc/Excel have no export variant.
fn file_format_to_export_format(f: FileFormat) -> Option<ExportFormat> {
    match f {
        FileFormat::Parquet => Some(ExportFormat::Parquet),
        FileFormat::Csv | FileFormat::Tsv | FileFormat::Psv => Some(ExportFormat::Csv),
        FileFormat::Json => Some(ExportFormat::Json),
        FileFormat::Jsonl => Some(ExportFormat::Ndjson),
        FileFormat::Arrow => Some(ExportFormat::Ipc),
        FileFormat::Avro => Some(ExportFormat::Avro),
        FileFormat::Orc | FileFormat::Excel => None,
    }
}

#[cfg(test)]
mod export_format_tests {
    use super::*;
    use std::path::Path;

    fn opts() -> OpenOptions {
        OpenOptions::default()
    }

    #[test]
    fn a_collect_in_flight_serves_the_frame_but_not_a_changed_frame() {
        use crate::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
        use polars::prelude::IntoLazy;

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        let lf = polars::df!("a" => (0..100).collect::<Vec<i32>>())
            .unwrap()
            .lazy();
        let mut state = DataTableState::from_lazyframe(lf, &opts()).unwrap();
        state.visible_rows = 10;
        let dataset = state.len_generation();
        app.data_table_state = Some(state);
        let generation = app.task_generation;
        app.collect_inflight = Some(InflightCollect {
            began: std::time::Instant::now(),
            files: None,
            generation,
            dataset,
            start: 0,
            end: 50,
            waited_on: true,
        });

        // The frame that sized the table asks again: the collect on its way covers
        // the view, so nothing new is planned.
        assert!(app.spawn_async_collect(App::LOADING_BUFFER));
        assert_eq!(app.task_generation, generation);

        // A filter changes the data underneath; those rows no longer answer.
        app.event(&AppEvent::Filter(vec![FilterStatement {
            column: "a".to_string(),
            operator: FilterOperator::Lt,
            value: "50".to_string(),
            logical_op: LogicalOperator::And,
        }]));
        assert_ne!(
            app.task_generation, generation,
            "a fresh collect was planned"
        );
        let inflight = app.collect_inflight.expect("the new collect is recorded");
        assert_eq!(inflight.generation, app.task_generation);
        assert_ne!(inflight.dataset, dataset);
    }

    #[test]
    fn a_short_read_on_a_remote_scan_is_the_count() {
        use crate::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
        use polars::prelude::IntoLazy;

        // A frame whose len() cannot be taken: a short read has to answer without it.
        let unreadable = LazyFrame::scan_parquet(
            polars::prelude::PlRefPath::new("/nonexistent/for-this-test.parquet"),
            Default::default(),
        )
        .unwrap();
        let job = LenCount {
            len_generation: 7,
            count_dir: None,
            files: None,
            lf: unreadable,
            streaming: false,
            meter: Arc::new(crate::measurements::Meter::default()),
        };
        let rows = |counted: Result<Counted, ()>| counted.map(|c| c.rows);
        assert_eq!(
            rows(job.after_collect(1_000, 30, 70)),
            Ok(1_030),
            "short: known"
        );
        assert_eq!(
            rows(job.after_collect(1_000, 70, 70)),
            Err(()),
            "full: counted"
        );
        assert_eq!(
            rows(job.after_collect(1_000, 0, 70)),
            Err(()),
            "deep and empty: counted"
        );

        // Through the harness: a filtered remote frame gets its count from the
        // collect that came back short, and the len() never runs alongside it.
        let (tx, rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        let lf = polars::df!("a" => (0..100).collect::<Vec<i32>>())
            .unwrap()
            .lazy();
        let mut state = DataTableState::from_lazyframe(lf, &opts()).unwrap();
        state.set_remote_source();
        state.visible_rows = 10;
        state.defer_collect = true;
        state.filter(vec![FilterStatement {
            column: "a".to_string(),
            operator: FilterOperator::Lt,
            value: "50".to_string(),
            logical_op: LogicalOperator::And,
        }]);
        assert!(!state.is_num_rows_valid());
        let dataset = state.len_generation();
        app.data_table_state = Some(state);
        assert!(app.spawn_async_collect("Filtering..."));
        assert_eq!(app.len_count_inflight, Some(dataset));

        let wait = std::time::Duration::from_secs(20);
        let first = rx.recv_timeout(wait).expect("the collect lands");
        assert!(
            matches!(first, AppEvent::BackgroundCollectReady { .. }),
            "the buffer comes first"
        );
        let second = rx.recv_timeout(wait).expect("the count follows");
        assert!(
            matches!(
                second,
                AppEvent::BackgroundLenReady {
                    len_generation,
                    num_rows: 50,
                    ..
                } if len_generation == dataset
            ),
            "the short read of 50 rows is the count"
        );
        app.event(&first);
        app.event(&second);
        let state = app.data_table_state.as_ref().unwrap();
        assert_eq!(state.num_rows_if_valid(), Some(50));
        assert_eq!(app.len_count_inflight, None);
    }

    #[test]
    fn end_on_an_uncounted_remote_dataset_waits_for_the_count() {
        use polars::prelude::IntoLazy;
        let (tx, rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        let lf = polars::df!("a" => (0..1_000).collect::<Vec<i32>>())
            .unwrap()
            .lazy();
        let whole = lf.clone();
        let mut state = DataTableState::from_lazyframe(lf, &opts()).unwrap();
        state.set_remote_source();
        state.set_remote_files(crate::widgets::datatable::RemoteFiles {
            urls: Arc::new(vec!["one".to_string(), "two".to_string()]),
            scan: Arc::new(
                move |urls: &[String], _as_text: &[polars::prelude::PlSmallStr]| {
                    Ok(if urls.len() == 2 {
                        whole.clone()
                    } else if urls[0] == "one" {
                        whole.clone().slice(0, 400)
                    } else {
                        whole.clone().slice(400, 600)
                    })
                },
            ),
            count: Arc::new(|| Ok(vec![vec![400], vec![300, 300]])),
            offsets: None,
        });
        state.visible_rows = 10;
        let dataset = state.len_generation();
        app.data_table_state = Some(state);

        // No jump to a guess: the count starts, and nothing is busy.
        assert!(app.jump_key(AppEvent::DoScrollEnd).is_none());
        assert!(!app.busy);
        assert_eq!(app.end_after_count, Some(dataset));
        assert_eq!(app.len_count_inflight, Some(dataset));
        assert_eq!(app.data_table_state.as_ref().unwrap().start_row, 0);

        let counted = rx
            .recv_timeout(std::time::Duration::from_secs(20))
            .expect("the count");
        assert!(matches!(
            &counted,
            AppEvent::BackgroundLenReady {
                num_rows: 1_000,
                file_row_groups: Some(_),
                ..
            }
        ));
        // With the count in, End goes.
        let next = app.event(&counted);
        assert!(
            matches!(next, Some(AppEvent::DoScrollEnd)),
            "the jump follows the count"
        );
        assert_eq!(app.end_after_count, None);
        let state = app.data_table_state.as_ref().unwrap();
        assert_eq!(state.num_rows_if_valid(), Some(1_000));
    }

    #[test]
    fn a_filter_applied_from_the_end_shows_its_rows() {
        // End on a 10,000-row remote object, then a filter matching 100 rows: the view
        // comes back to the top and the count is the filter's, not the old position.
        use crate::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
        use polars::prelude::IntoLazy;

        let (tx, rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        let lf = polars::df!("a" => (0..10_000).collect::<Vec<i32>>())
            .unwrap()
            .lazy();
        let mut state = DataTableState::from_lazyframe(lf, &opts()).unwrap();
        state.set_remote_source();
        state.set_row_groups(&[10_000]);
        state.visible_rows = 10;
        state.defer_collect = true;
        assert!(state.scroll_to_end());
        app.data_table_state = Some(state);

        app.event(&AppEvent::Filter(vec![FilterStatement {
            column: "a".to_string(),
            operator: FilterOperator::Lt,
            value: "100".to_string(),
            logical_op: LogicalOperator::And,
        }]));
        let wait = std::time::Duration::from_secs(20);
        for _ in 0..2 {
            let event = rx.recv_timeout(wait).expect("the collect, then the count");
            app.event(&event);
        }
        let state = app.data_table_state.as_ref().unwrap();
        assert_eq!(state.num_rows_if_valid(), Some(100));
        assert_eq!(state.start_row, 0);
        assert!(
            state.buffered_start() == 0 && state.buffered_end() >= 10,
            "the first page is on hand: {}..{}",
            state.buffered_start(),
            state.buffered_end()
        );
    }

    #[test]
    fn compressed_csv_still_defaults_to_csv() {
        // `sales.csv.gz` has extension `gz`; the `.csv` that decides this is in the
        // stem. Reading the extension alone offered no export default at all.
        assert_eq!(
            App::export_format_for(Path::new("sales.csv.gz"), &opts()),
            Some(ExportFormat::Csv)
        );
        assert_eq!(
            App::export_format_for(Path::new("sales.csv.zst"), &opts()),
            Some(ExportFormat::Csv)
        );
    }

    #[test]
    fn plain_extensions_map_to_their_formats() {
        for (name, expected) in [
            ("a.parquet", Some(ExportFormat::Parquet)),
            ("a.csv", Some(ExportFormat::Csv)),
            ("a.tsv", Some(ExportFormat::Csv)),
            ("a.json", Some(ExportFormat::Json)),
            ("a.ndjson", Some(ExportFormat::Ndjson)),
            ("a.jsonl", Some(ExportFormat::Ndjson)),
            ("a.arrow", Some(ExportFormat::Ipc)),
            ("a.feather", Some(ExportFormat::Ipc)),
            ("a.avro", Some(ExportFormat::Avro)),
            ("a.xlsx", None),
            ("a.orc", None),
            ("a.unknown", None),
        ] {
            assert_eq!(
                App::export_format_for(Path::new(name), &opts()),
                expected,
                "{name}"
            );
        }
    }

    #[test]
    fn explicit_format_beats_the_extension() {
        let mut options = opts();
        options.format = Some(FileFormat::Parquet);
        assert_eq!(
            App::export_format_for(Path::new("mislabelled.csv"), &options),
            Some(ExportFormat::Parquet)
        );
    }
}

#[cfg(test)]
mod probe_slot_tests {
    use super::*;
    use std::sync::mpsc;

    /// The directory browsed into is the whole screen, so its listing never waits for
    /// a slot held by roots the user has left.
    #[test]
    fn the_directory_browsed_into_is_never_held_behind_the_cap() {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.home.network_check = |_| true;
        let dir = PathBuf::from("/pretend/share/raw");
        app.home.browsing = Some(dir.clone());
        app.home_probes_inflight = (0..MAX_CONCURRENT_PROBES)
            .map(|i| PathBuf::from(format!("/pretend/slow{i}")))
            .collect();

        app.spawn_home_probes();

        assert!(app.home_probes_inflight.contains(&dir));
    }

    /// Rows read so far show, marked as still listing, until the listing lands; a batch
    /// arriving after the whole answer is dropped. A listing cut at the cap says so.
    #[test]
    fn rows_so_far_show_until_the_listing_lands() {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.home.network_check = |_| true;
        let dir = PathBuf::from("/pretend/share/raw");
        app.home.browsing = Some(dir.clone());
        app.home_probes_inflight = vec![dir.clone()];
        let row = |name: &str| discover::Entry::directory(&dir.join(name));

        app.event(&AppEvent::HomeProbeProgress {
            root: dir.clone(),
            rows: vec![row("2009-01-03")],
        });
        app.home.rebuild(&[], &[]);
        let section = &app.home.sections[0];
        assert!(section.waiting);
        assert_eq!(section.subtitle.as_deref(), Some("1 so far"));
        assert_eq!(section.rows.len(), 1);

        app.event(&AppEvent::HomeProbeReady {
            root: dir.clone(),
            rows: Some(vec![row("2009-01-03"), row("2009-01-04")]),
            cut_short: true,
        });
        app.event(&AppEvent::HomeProbeProgress {
            root: dir.clone(),
            rows: vec![row("late")],
        });
        app.home.rebuild(&[], &[]);
        let section = &app.home.sections[0];
        assert!(!section.waiting);
        assert_eq!(section.rows.len(), 2);
        assert_eq!(section.subtitle.as_deref(), Some("first 5,000"));
    }

    /// A probe that answers must give its slot back. The cap is there to bound threads
    /// wedged on a dead mount, and those never answer at all; counting completed probes
    /// against it meant that after MAX_CONCURRENT_PROBES roots, no root was ever probed
    /// again for the rest of the session. Roots accumulate as datasets are opened on
    /// different mounts, so this is reached by ordinary use, and it shows as a network
    /// section that stays empty with no error.
    #[test]
    fn an_answered_probe_frees_its_slot() {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());

        let roots: Vec<PathBuf> = (0..MAX_CONCURRENT_PROBES)
            .map(|i| PathBuf::from(format!("/pretend/remote{i}")))
            .collect();
        app.home_probes_inflight = roots.clone();

        for (i, root) in roots.iter().enumerate() {
            // Alternate the two ways a probe can answer; both are answers.
            let rows = if i % 2 == 0 { Some(Vec::new()) } else { None };
            app.event(&AppEvent::HomeProbeReady {
                root: root.clone(),
                rows,
                cut_short: false,
            });
        }

        assert!(
            app.home_probes_inflight.is_empty(),
            "every probe answered, so nothing should still hold a slot: {:?}",
            app.home_probes_inflight
        );
    }

    /// A root that never answers keeps its slot, which is the whole point of the cap.
    #[test]
    fn an_unanswered_probe_keeps_its_slot() {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());

        let wedged = PathBuf::from("/pretend/dead-mount");
        let answered = PathBuf::from("/pretend/live-mount");
        app.home_probes_inflight = vec![wedged.clone(), answered.clone()];

        app.event(&AppEvent::HomeProbeReady {
            root: answered,
            rows: Some(Vec::new()),
            cut_short: false,
        });

        assert_eq!(
            app.home_probes_inflight,
            vec![wedged],
            "a thread still stuck on a dead mount must keep costing a slot"
        );
    }
}

#[cfg(test)]
mod classify_batch_tests {
    use super::*;
    use std::sync::mpsc;

    /// A listing of `n` subdirectories nothing has looked into, under a directory that
    /// does not exist — so a pass over them settles nothing and blocks on nothing.
    fn unlooked_at(n: usize) -> home::Listing {
        let rows = (0..n)
            .map(|i| {
                let mut entry =
                    discover::Entry::directory(&PathBuf::from(format!("/pretend/share/d{i:04}")));
                entry.kind = discover::EntryKind::Unknown;
                entry
            })
            .collect();
        home::Listing {
            sections: vec![home::Section {
                door: None,
                title: "SHARE".into(),
                subtitle: None,
                origin: None,
                rows,
                unavailable: false,
                unavailable_note: None,
                folded_by_default: false,
                remote_root: None,
                waiting: false,
                grouped_by_place: false,
                place_labels: Default::default(),
                root: None,
            }],
            ..Default::default()
        }
    }

    /// Put the viewport where a frame of twenty rows would put it to show `selected`,
    /// exactly as `render_list` does. The two move together, so a test that set one
    /// and not the other would describe a screen that cannot exist.
    fn looking_at(app: &mut App, selected: usize) {
        app.home.selected = selected;
        app.home.view_height = 20;
        app.home.scroll = selected.saturating_sub(17);
    }

    /// Paging quickly must not leave a classification queued for every row it went
    /// past. Only one pass is ever out, and the next one is chosen from the viewport
    /// as it is when that one lands — so a page that crossed four hundred rows asks
    /// about the forty it stopped on.
    #[test]
    fn only_one_classification_pass_is_out_at_a_time() {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.home.apply_listing(unlooked_at(500));
        looking_at(&mut app, 18);

        app.request_home_classifications();
        assert!(
            app.home.classify_in_flight,
            "the first pass should have gone out"
        );

        // Paging while it is out. Nothing more is asked for meanwhile.
        for row in [117, 217, 317, 417] {
            looking_at(&mut app, row);
            app.request_home_classifications();
        }
        assert!(app.home.classify_in_flight, "and still only the one");

        // It lands, and what follows it is about where the viewport is now.
        app.event(&AppEvent::HomeClassified {
            measured: Vec::new(),
            done: true,
        });
        let next = app.home.unclassified_visible(CLASSIFY_BATCH);
        assert!(
            next.iter()
                .all(|e| e.name.trim_start_matches('d').parse::<usize>().unwrap() >= 300),
            "the next pass follows the viewport, not the rows paged over: {:?}",
            next.iter().map(|e| &e.name).collect::<Vec<_>>()
        );
    }

    /// A pass that answers must give the slot back, or the home screen stops
    /// classifying anything for the rest of the session.
    #[test]
    fn an_answered_pass_frees_the_slot() {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.home.classify_in_flight = true;

        app.event(&AppEvent::HomeClassified {
            measured: Vec::new(),
            done: true,
        });

        assert!(!app.home.classify_in_flight);
    }

    /// A pass that lands after the listing was rebuilt still labels its row. A probe
    /// or a cloud peek landing rebuilds the listing, and a Recent section with a few
    /// buckets in it lands several in a row: dropping the answer each time left a share
    /// dataset unlabeled for half a minute.
    #[test]
    fn a_pass_that_outlives_its_listing_still_counts() {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.home.apply_listing(unlooked_at(4));
        let path = PathBuf::from("/pretend/share/d0000");
        app.home.classify_in_flight = true;

        // What a rebuild does to the generation while the pass is out.
        app.home_generation = app.home_generation.wrapping_add(1);
        app.event(&AppEvent::HomeClassified {
            measured: vec![(
                path.clone(),
                home::Measured {
                    kind: Some(discover::EntryKind::Hive),
                    ..Default::default()
                },
            )],
            done: true,
        });

        let kind = app.home.visible().iter().find_map(|row| match row {
            home::Row::Entry { entry, .. } if entry.path == path => Some(entry.kind),
            _ => None,
        });
        assert_eq!(kind, Some(discover::EntryKind::Hive));
    }

    /// Space folds the header under the cursor, and never starts a filter: a filter of
    /// one space is invisible at the prompt and searched below the working directory.
    /// Once typing has started it types.
    #[test]
    fn space_folds_a_header_and_types_only_mid_filter() {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.input_mode = InputMode::Home;
        app.home.apply_listing(unlooked_at(3));
        let space = || AppEvent::Key(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE));
        app.home.selected = 0;
        assert!(app.home.selection_is_header());

        app.event(&space());
        assert!(app.home.is_collapsed(0));
        app.event(&space());
        assert!(!app.home.is_collapsed(0));

        app.home.selected = 1;
        app.event(&space());
        assert_eq!(app.home.filter, "", "a space on a row is nothing");
        assert!(!app.home.is_collapsed(0));

        app.event(&AppEvent::Key(KeyEvent::new(
            KeyCode::Char('d'),
            KeyModifiers::NONE,
        )));
        app.event(&space());
        assert_eq!(app.home.filter, "d ");
    }

    /// A row's label shows as soon as its answer lands, not when the batch it was in
    /// finishes; the slot stays taken until then, so a second batch never overlaps it.
    #[test]
    fn a_label_lands_before_its_batch_is_done() {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.home.apply_listing(unlooked_at(4));
        let path = PathBuf::from("/pretend/share/d0000");
        app.home.classify_in_flight = true;

        app.event(&AppEvent::HomeClassified {
            measured: vec![(
                path.clone(),
                home::Measured {
                    kind: Some(discover::EntryKind::Hive),
                    ..Default::default()
                },
            )],
            done: false,
        });

        let kind = app.home.visible().iter().find_map(|row| match row {
            home::Row::Entry { entry, .. } if entry.path == path => Some(entry.kind),
            _ => None,
        });
        assert_eq!(kind, Some(discover::EntryKind::Hive));
        assert!(app.home.classify_in_flight, "the batch is still out");
    }
}

#[cfg(test)]
mod quality_sample_tests {
    use super::*;
    use std::sync::mpsc;

    /// Choosing equal rows per value of a column sets the grain to that column, so
    /// Segments and Trends have what the sample was drawn for; a grain chosen
    /// afterwards is not taken back.
    #[test]
    fn the_grain_follows_an_equal_per_value_sample_once() {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        let per_date = sampling::SampleMethod::PerPartition {
            column: "date".into(),
        };
        app.analysis_modal.sample.method = per_date.clone();
        app.sync_quality_plan();
        assert_eq!(
            app.analysis_modal.data_quality_plan.grain,
            data_quality::QualityGrain::Partition("date".into())
        );

        app.analysis_modal.data_quality_plan.grain = data_quality::QualityGrain::Dataset;
        app.sync_quality_plan();
        assert_eq!(
            app.analysis_modal.data_quality_plan.grain,
            data_quality::QualityGrain::Dataset,
            "the same sample again leaves the chosen grain alone"
        );
    }
}

#[cfg(test)]
mod chart_prepare_tests {
    use super::*;
    use std::sync::mpsc;

    fn histogram_request(column: &str) -> ChartRequest {
        ChartRequest::Histogram {
            column: column.to_string(),
            bins: 10,
            row_limit: None,
        }
    }

    fn prepared_histogram(column: &str) -> ChartPrepared {
        ChartPrepared::Histogram(chart_data::HistogramData {
            column: column.to_string(),
            bins: Vec::new(),
            x_min: 0.0,
            x_max: 1.0,
            max_count: 0.0,
        })
    }

    fn inflight(request: &ChartRequest) -> ChartInflight {
        ChartInflight {
            dataset: None,
            request: request.clone(),
            stale: false,
        }
    }

    /// A result computed against a dataset that is no longer the one open is dropped
    /// even when the record is still current.
    #[test]
    fn a_result_for_another_dataset_is_dropped() {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        let request = histogram_request("a");
        app.chart_inflight = Some(ChartInflight {
            dataset: Some(12345),
            ..inflight(&request)
        });

        *app.pending_chart_result.lock().unwrap() = Some(Ok(prepared_histogram("a")));
        app.event(&AppEvent::BackgroundChartReady);
        assert!(!app.chart_cache.satisfies(&request));
        assert!(app.chart_inflight.is_none());
    }

    /// Leaving the dataset drops the chart state with it: nothing keeps spinning on the
    /// home screen, the worker still running is waited for and its result discarded
    /// (nothing else starts until it lands), and an export parked on data that will
    /// never come stops holding the app busy.
    #[test]
    fn leaving_the_dataset_resets_chart_state() {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        let request = histogram_request("a");
        app.chart_inflight = Some(inflight(&request));
        app.chart_export_waiting = Some((
            PathBuf::from("/tmp/x.png"),
            ChartExportFormat::Png,
            String::new(),
            1,
            1,
        ));
        app.busy = true;

        app.abandon_load();
        assert!(!app.chart_preparing(), "nothing spins on the home screen");
        assert!(
            app.chart_inflight.as_ref().is_some_and(|i| i.stale),
            "the worker cannot be cancelled, so it is remembered as stale"
        );
        assert!(app.pending_chart_result.lock().unwrap().is_none());
        assert!(app.chart_export_waiting.is_none());
        assert!(!app.is_busy());

        *app.pending_chart_result.lock().unwrap() = Some(Ok(prepared_histogram("a")));
        app.event(&AppEvent::BackgroundChartReady);
        assert!(
            !app.chart_cache.satisfies(&request),
            "stale result is dropped"
        );
        assert!(app.chart_inflight.is_none(), "and the slot is free again");
    }

    /// Going home in the one-frame window between `ChartExport` arming `busy` and the
    /// deferred `DoChartExport`: the export must not be parked on a view that is gone,
    /// leaving the home screen busy forever.
    #[test]
    fn a_chart_export_deferred_past_the_chart_view_releases_busy() {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        let path = PathBuf::from("/tmp/x.png");
        let next = app
            .event(&AppEvent::ChartExport(
                path,
                ChartExportFormat::Png,
                String::new(),
                1,
                1,
            ))
            .expect("ChartExport defers to DoChartExport");
        assert!(app.is_busy());

        app.enter_home();
        app.event(&next);
        assert!(!app.is_busy());
        assert!(matches!(app.loading_state, LoadingState::Idle));
        assert!(app.chart_export_waiting.is_none());
    }

    /// Going home while the export file is being written: the app stops being busy,
    /// and when the write finishes its result is ignored rather than reopening the
    /// export modal over the home screen.
    #[test]
    fn leaving_the_dataset_abandons_an_export_write() {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        let path = PathBuf::from("/tmp/x.png");
        app.chart_export_generation = 7;
        app.chart_export_inflight = Some(7);
        app.busy = true;
        app.loading_state = LoadingState::Exporting {
            file_path: path.clone(),
            current_phase: "Exporting chart".to_string(),
            progress_percent: 0,
        };
        let task_generation = app.task_generation();

        app.abandon_load();
        assert!(!app.is_busy());
        assert!(matches!(app.loading_state, LoadingState::Idle));
        assert_eq!(app.task_generation(), task_generation);

        app.event(&AppEvent::BackgroundChartExportWritten {
            generation: 7,
            path,
            format: ChartExportFormat::Png,
            result: Err("disk full".to_string()),
        });
        assert!(!app.error_modal.active);
        assert!(!app.chart_export_modal.active);
        assert!(!app.is_busy());
    }

    /// A selection that cannot be charted is remembered as failed rather than retried
    /// after every event, which would spin the throbber forever.
    #[test]
    fn a_failed_preparation_is_remembered_not_retried() {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        let request = histogram_request("a");
        app.chart_inflight = Some(inflight(&request));

        *app.pending_chart_result.lock().unwrap() = Some(Err("duplicate column".into()));
        app.event(&AppEvent::BackgroundChartReady);
        assert!(app.chart_inflight.is_none());
        assert!(matches!(
            app.chart_cache.get(&request),
            Some(Err(m)) if m == "duplicate column"
        ));
        assert!(!app.chart_cache.satisfies(&request));

        // With that selection on screen, nothing more is wanted.
        app.input_mode = InputMode::Chart;
        app.chart_modal.active = true;
        app.chart_modal.chart_kind = ChartKind::Histogram;
        app.chart_modal.hist_column = Some("a".to_string());
        app.chart_modal.hist_bins = 10;
        app.chart_modal.row_limit = None;
        assert_eq!(ChartRequest::from_modal(&app.chart_modal), Some(request));
        assert!(!app.chart_request_pending(), "not asked for again");
    }

    /// Two selections that alternate stay prepared: neither is collected again when
    /// the user toggles between them, whether they succeeded or failed.
    #[test]
    fn alternating_selections_keep_their_entries() {
        let mut cache = ChartCache::default();
        let a = histogram_request("a");
        let b = histogram_request("b");
        cache.insert(a.clone(), Ok(prepared_histogram("a")));
        cache.insert(b.clone(), Err("no numbers".into()));
        assert!(cache.satisfies(&a));
        assert!(matches!(cache.get(&b), Some(Err(m)) if m == "no numbers"));

        // Re-inserting replaces rather than duplicates, and moves to the back.
        cache.insert(a.clone(), Ok(prepared_histogram("a")));
        assert_eq!(cache.entries.len(), 2);

        // Fill to capacity, then one more evicts the least recently used: `b`.
        for i in 0..ChartCache::CAPACITY - 2 {
            cache.insert(
                histogram_request(&format!("c{i}")),
                Ok(prepared_histogram("c")),
            );
        }
        assert_eq!(cache.entries.len(), ChartCache::CAPACITY);
        assert!(cache.get(&b).is_some());
        cache.insert(histogram_request("one more"), Ok(prepared_histogram("d")));
        assert_eq!(cache.entries.len(), ChartCache::CAPACITY);
        assert!(cache.get(&b).is_none(), "the least recently used went");
        assert!(cache.satisfies(&a), "the refreshed one is still there");
    }

    fn xy_request(x: &str) -> ChartRequest {
        ChartRequest::XY {
            x_column: x.to_string(),
            y_columns: vec!["y".to_string()],
            row_limit: None,
        }
    }

    fn prepared_xy(x: &str) -> ChartPrepared {
        ChartPrepared::XY(ChartCacheXY {
            x_column: x.to_string(),
            y_columns: vec!["y".to_string()],
            series: vec![vec![(0.0, 1.0)]],
            series_log: None,
            x_axis_kind: chart_data::XAxisTemporalKind::Numeric,
        })
    }

    fn has_log_series(cache: &ChartCache, request: &ChartRequest) -> bool {
        matches!(
            cache.prepared(request),
            Some(ChartPrepared::XY(xy)) if xy.series_log.is_some()
        )
    }

    /// XY series are the payload that grows with the data, so fewer of them are kept
    /// than small kinds, the one on screen is kept over one merely inserted later, and
    /// only the one on screen carries a log-scale copy.
    #[test]
    fn xy_entries_are_few_and_the_one_on_screen_stays() {
        let mut cache = ChartCache::default();
        let (a, b, c) = (xy_request("a"), xy_request("b"), xy_request("c"));
        cache.insert(a.clone(), Ok(prepared_xy("a")));
        cache.insert(b.clone(), Ok(prepared_xy("b")));
        cache.touch(&a, true);
        assert!(has_log_series(&cache, &a));
        assert!(!has_log_series(&cache, &b));

        cache.insert(c.clone(), Ok(prepared_xy("c")));
        assert!(cache.satisfies(&a), "on screen, so kept");
        assert!(!cache.satisfies(&b), "least recently used XY went");
        assert!(cache.satisfies(&c));
        assert_eq!(cache.entries.len(), ChartCache::XY_CAPACITY);

        // Small kinds are not counted against the XY cap, and vice versa.
        cache.insert(histogram_request("h"), Ok(prepared_histogram("h")));
        assert_eq!(cache.entries.len(), 3);

        cache.touch(&c, true);
        assert!(has_log_series(&cache, &c));
        assert!(
            !has_log_series(&cache, &a),
            "only the one on screen keeps its log copy"
        );
    }

    /// Writes a CSV with columns x and y where y = x * factor, so two datasets share a
    /// schema but not values.
    fn write_xy_csv(dir: &std::path::Path, name: &str, factor: i64) -> PathBuf {
        let path = dir.join(name);
        let mut body = String::from("x,y\n");
        for x in 0..5i64 {
            body.push_str(&format!("{x},{}\n", x * factor));
        }
        std::fs::write(&path, body).unwrap();
        path
    }

    /// Drive background results back into the app until `done`.
    pub(super) fn pump(
        app: &mut App,
        rx: &mpsc::Receiver<AppEvent>,
        tx: &mpsc::Sender<AppEvent>,
        done: impl Fn(&App) -> bool,
    ) {
        for _ in 0..500 {
            while let Ok(ev) = rx.try_recv() {
                if let Some(next) = app.event(&ev) {
                    let _ = tx.send(next);
                }
            }
            if done(app) {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("app did not reach the expected state within 5 seconds");
    }

    pub(super) fn open(
        app: &mut App,
        rx: &mpsc::Receiver<AppEvent>,
        tx: &mpsc::Sender<AppEvent>,
        path: PathBuf,
    ) {
        // As `home_open_path` does before it emits the `Open`.
        app.input_mode = InputMode::Normal;
        if let Some(next) = app.event(&AppEvent::Open(vec![path], OpenOptions::default())) {
            let _ = tx.send(next);
        }
        pump(app, rx, tx, |a| {
            a.data_table_state.is_some() && !a.is_busy()
        });
    }

    fn select_xy(app: &mut App) {
        app.event(&AppEvent::Key(KeyEvent::new(
            KeyCode::Char('c'),
            KeyModifiers::NONE,
        )));
        assert_eq!(app.input_mode, InputMode::Chart);
        app.chart_modal.x_column = Some("x".to_string());
        app.chart_modal.y_columns = vec!["y".to_string()];
        app.event(&AppEvent::Resize(80, 24));
    }

    /// A chart still being prepared when the user goes home and opens another file with
    /// the same columns must not land in the new dataset; the new dataset's own values
    /// are what gets charted.
    #[test]
    fn a_prepare_from_the_previous_dataset_does_not_land_in_the_next() {
        crate::tests::ensure_sample_data();
        let dir = tempfile::tempdir().unwrap();
        let first = write_xy_csv(dir.path(), "first.csv", 1);
        let second = write_xy_csv(dir.path(), "second.csv", 100);
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx.clone(), crate::tests::test_runtime());

        open(&mut app, &rx, &tx, first);
        select_xy(&mut app);
        assert!(app.chart_preparing());

        app.event(&AppEvent::Key(KeyEvent::new(
            KeyCode::Char('o'),
            KeyModifiers::CONTROL,
        )));
        assert_eq!(app.input_mode, InputMode::Home);
        assert!(!app.chart_preparing(), "nothing spins on the home screen");

        open(&mut app, &rx, &tx, second);
        select_xy(&mut app);
        pump(&mut app, &rx, &tx, |a| a.chart_data_ready());

        let request = ChartRequest::from_modal(&app.chart_modal).unwrap();
        let Some(ChartPrepared::XY(xy)) = app.chart_cache.prepared(&request) else {
            panic!("an XY chart is prepared");
        };
        assert_eq!(xy.series[0][4], (4.0, 400.0), "the second dataset's values");
        assert!(!app.chart_preparing());
    }

    /// A worker that dies without a result (a panic in the preparation) must not leave
    /// the in-flight record standing for the rest of the session: the `Ready` event is
    /// sent regardless, and an empty slot is recorded as a failure.
    #[test]
    fn a_ready_event_with_no_result_clears_the_inflight_record() {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        let request = histogram_request("a");
        app.chart_inflight = Some(inflight(&request));
        assert!(app.pending_chart_result.lock().unwrap().is_none());

        app.event(&AppEvent::BackgroundChartReady);
        assert!(app.chart_inflight.is_none());
        assert!(matches!(app.chart_cache.get(&request), Some(Err(_))));
    }

    /// Esc leaves a worker running that cannot be cancelled; reopening the chart and
    /// selecting again queues the new request behind it. The user is waiting on a
    /// computation, so the throbber must show, and the request must then be prepared.
    #[test]
    fn a_reselection_behind_a_stale_worker_counts_as_preparing() {
        crate::tests::ensure_sample_data();
        let dir = tempfile::tempdir().unwrap();
        let path = write_xy_csv(dir.path(), "reselect.csv", 3);
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx.clone(), crate::tests::test_runtime());
        open(&mut app, &rx, &tx, path);

        select_xy(&mut app);
        assert!(app.chart_preparing());
        app.event(&AppEvent::Key(KeyEvent::new(
            KeyCode::Esc,
            KeyModifiers::NONE,
        )));
        assert_eq!(app.input_mode, InputMode::Normal);
        assert!(
            !app.chart_preparing(),
            "nothing is wanted while the chart is closed"
        );
        assert!(
            app.chart_inflight.as_ref().is_some_and(|i| i.stale),
            "the orphaned worker is still remembered"
        );

        select_xy(&mut app);
        assert!(
            app.chart_preparing(),
            "a request waiting behind the orphan is being prepared, in effect"
        );
        pump(&mut app, &rx, &tx, |a| a.chart_data_ready());
        assert!(!app.chart_preparing());
    }
}

#[cfg(test)]
mod template_rollback_tests {
    use super::chart_prepare_tests::open;
    use super::*;
    use std::sync::mpsc;

    /// A template that pivots and then fails must roll the pivot back too: otherwise the
    /// view shows the original columns while SQL still runs against the pivot.
    #[test]
    fn a_failed_template_rolls_back_the_reshape() {
        crate::tests::ensure_sample_data();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("long.csv");
        let mut body = String::from("id,key,val\n");
        for id in 0..5 {
            body.push_str(&format!("{id},k1,{id}\n{id},k2,{}\n", id * 10));
        }
        std::fs::write(&path, body).unwrap();
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx.clone(), crate::tests::test_runtime());
        open(&mut app, &rx, &tx, path);

        let mut template = app
            .create_template_from_current_state(
                "pivot then break".to_string(),
                None,
                template::MatchCriteria {
                    exact_path: None,
                    relative_path: None,
                    path_pattern: None,
                    filename_pattern: None,
                    schema_columns: None,
                    schema_types: None,
                },
            )
            .unwrap();
        template.settings.pivot = Some(PivotSpec {
            index: vec!["id".to_string()],
            pivot_column: "key".to_string(),
            value_column: "val".to_string(),
            aggregation: pivot_melt_modal::PivotAggregation::First,
            sort_columns: None,
        });
        // Applied after the pivot, and referring to a column that does not exist.
        template.settings.column_order = vec!["no_such_column".to_string()];

        assert!(app.apply_template(&template).is_err());

        let state = app.data_table_state.as_ref().unwrap();
        let root: Vec<String> = state
            .query_root()
            .collect_schema()
            .unwrap()
            .iter_names()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(
            root,
            vec!["id", "key", "val"],
            "SQL root is the loaded data again"
        );
        assert!(state.last_pivot_spec().is_none());
        assert!(state.reshaped_lf_clone().is_none());
    }

    /// Rolling a failed template back restores the frame, and the frame's rows still
    /// stand for rows of a file — so what the state believes about them has to be
    /// rolled back with it, or the cells go back to reading as plain nulls.
    #[test]
    fn a_failed_template_rolls_back_what_the_rows_knew() {
        use polars::prelude::{ParquetWriter, df};
        let dir = tempfile::tempdir().unwrap();
        let write = |sub: &str, mut frame: polars::prelude::DataFrame| {
            let d = dir.path().join(sub);
            std::fs::create_dir_all(&d).unwrap();
            let f = std::fs::File::create(d.join("data.parquet")).unwrap();
            ParquetWriter::new(f).finish(&mut frame).unwrap();
        };
        write("date=2024-01-01", df!("id" => &[1i64, 4]).unwrap());
        write(
            "date=2024-01-02",
            df!("id" => &[2i64, 3], "extra" => &["x", "y"]).unwrap(),
        );

        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx.clone(), crate::tests::test_runtime());
        app.input_mode = InputMode::Normal;
        let opts = OpenOptions {
            hive: true,
            ..OpenOptions::default()
        };
        if let Some(next) = app.event(&AppEvent::Open(vec![dir.path().to_path_buf()], opts)) {
            let _ = tx.send(next);
        }
        super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| {
            a.data_table_state.is_some() && !a.is_busy()
        });
        assert!(
            app.data_table_state.as_ref().unwrap().drifts(),
            "the directory drifts to begin with"
        );

        let mut template = app
            .create_template_from_current_state(
                "query then break".to_string(),
                None,
                template::MatchCriteria {
                    exact_path: None,
                    relative_path: None,
                    path_pattern: None,
                    filename_pattern: None,
                    schema_columns: None,
                    schema_types: None,
                },
            )
            .unwrap();
        template.settings.sql_query = Some("select * from df".to_string());
        // Applied after the query, and referring to a column that does not exist.
        template.settings.column_order = vec!["no_such_column".to_string()];

        assert!(app.apply_template(&template).is_err());

        let state = app.data_table_state.as_ref().unwrap();
        assert!(
            state.drifts(),
            "the rollback puts back what the restored frame carries"
        );
        let names: Vec<&str> = state.schema.iter_names().map(|n| n.as_str()).collect();
        assert_eq!(
            names,
            ["date", "id", "extra"],
            "and no hidden column with it"
        );
    }

    /// The rollback puts back what the dataset said, not what the view was saying.
    ///
    /// A note about rows a sort is leaving out belongs to the sort. Snapshotting it
    /// with the dataset's own notes and handing it back on rollback made it permanent
    /// — it outlived the sort that earned it, and sorting again added a second copy —
    /// because the field it is handed back into is the one only a reset clears.
    #[test]
    fn a_failed_template_does_not_make_the_views_note_permanent() {
        use polars::prelude::{ParquetWriter, df};
        let dir = tempfile::tempdir().unwrap();
        let write = |sub: &str, mut frame: polars::prelude::DataFrame| {
            let d = dir.path().join(sub);
            std::fs::create_dir_all(&d).unwrap();
            let f = std::fs::File::create(d.join("data.parquet")).unwrap();
            ParquetWriter::new(f).finish(&mut frame).unwrap();
        };
        write(
            "date=2024-01-01",
            df!("id" => &[0i64, 1, 2], "n" => &[0i64, 1, 2]).unwrap(),
        );
        write(
            "date=2024-01-02",
            df!("id" => &[3i64, 4], "n" => &["x", "y"]).unwrap(),
        );

        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx.clone(), crate::tests::test_runtime());
        app.input_mode = InputMode::Normal;
        let opts = OpenOptions {
            hive: true,
            ..OpenOptions::default()
        };
        if let Some(next) = app.event(&AppEvent::Open(vec![dir.path().to_path_buf()], opts)) {
            let _ = tx.send(next);
        }
        super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| {
            a.data_table_state.is_some() && !a.is_busy()
        });

        let left_out = |app: &App| -> usize {
            app.data_table_state
                .as_ref()
                .unwrap()
                .notes()
                .iter()
                .filter(|note| note.summary.contains("is not read from"))
                .count()
        };

        app.data_table_state
            .as_mut()
            .unwrap()
            .sort(vec!["n".to_string()], true);
        assert_eq!(left_out(&app), 1, "the sort has something to say");

        let mut template = app
            .create_template_from_current_state(
                "query then break".to_string(),
                None,
                template::MatchCriteria {
                    exact_path: None,
                    relative_path: None,
                    path_pattern: None,
                    filename_pattern: None,
                    schema_columns: None,
                    schema_types: None,
                },
            )
            .unwrap();
        template.settings.sql_query = Some("select * from df".to_string());
        template.settings.column_order = vec!["no_such_column".to_string()];
        assert!(app.apply_template(&template).is_err());

        // The sort is back, so the rows it leaves out are back out — and the note has
        // to be back with them. A frame three rows short of the dataset with nothing
        // on screen saying why is the same fault as a note that outlives its sort,
        // seen from the other side.
        let state = app.data_table_state.as_ref().unwrap();
        assert_eq!(
            state.view_sort_columns(),
            ["n"],
            "the rollback puts the sort back"
        );
        assert_eq!(
            state.lf.clone().collect().unwrap().height(),
            3,
            "and the frame still leaves the two rows out"
        );
        assert_eq!(left_out(&app), 1, "so the note is still there to say so");

        app.data_table_state
            .as_mut()
            .unwrap()
            .sort(Vec::new(), true);
        assert_eq!(
            left_out(&app),
            0,
            "and clearing the sort takes it away, rollback or no rollback"
        );

        app.data_table_state
            .as_mut()
            .unwrap()
            .sort(vec!["n".to_string()], true);
        assert_eq!(left_out(&app), 1, "sorting again says it once, not twice");
    }

    /// Drilling into a group and back out puts the frame back; the note about what the
    /// frame leaves out has to come back with it.
    ///
    /// Reachable without a group-by: `is_grouped` is a dtype question — does any column
    /// hold a list — so a dataset written with a native List column is drillable as it
    /// stands, drift and all.
    #[test]
    fn drilling_back_up_puts_the_views_note_back_with_its_frame() {
        use polars::prelude::{IntoLazy, ParquetWriter, df};
        let dir = tempfile::tempdir().unwrap();
        let write = |sub: &str, frame: polars::prelude::DataFrame| {
            // Grouped into a List column, which is what makes the dataset drillable.
            let mut frame = frame
                .lazy()
                .group_by([col("id"), col("n")])
                .agg([col("v")])
                .sort(["id"], Default::default())
                .collect()
                .unwrap();
            let d = dir.path().join(sub);
            std::fs::create_dir_all(&d).unwrap();
            let f = std::fs::File::create(d.join("data.parquet")).unwrap();
            ParquetWriter::new(f).finish(&mut frame).unwrap();
        };
        write(
            "date=2024-01-01",
            df!("id" => &[0i64, 1, 2], "n" => &[0i64, 1, 2], "v" => &[10i64, 11, 12]).unwrap(),
        );
        // `n` as text here, so it is not read from this file.
        write(
            "date=2024-01-02",
            df!("id" => &[3i64, 4], "n" => &["x", "y"], "v" => &[13i64, 14]).unwrap(),
        );

        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx.clone(), crate::tests::test_runtime());
        app.input_mode = InputMode::Normal;
        let opts = OpenOptions {
            hive: true,
            ..OpenOptions::default()
        };
        if let Some(next) = app.event(&AppEvent::Open(vec![dir.path().to_path_buf()], opts)) {
            let _ = tx.send(next);
        }
        super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| {
            a.data_table_state.is_some() && !a.is_busy()
        });

        let state = app.data_table_state.as_mut().unwrap();
        assert!(state.is_grouped(), "a native List column, with no group-by");
        assert!(state.drifts(), "and the files disagree on `n`");

        let left_out = |s: &crate::widgets::datatable::DataTableState| {
            s.notes()
                .iter()
                .filter(|note| note.summary.contains("is not read from"))
                .count()
        };

        state.sort(vec!["n".to_string()], true);
        assert_eq!(
            state.lf.clone().collect().unwrap().height(),
            3,
            "the sort leaves the two rows of the text file out"
        );
        assert_eq!(left_out(state), 1, "and says so");

        state.table_state.select(Some(0));
        state.drill_down_into_group(0).unwrap();
        assert!(state.is_drilled_down());
        assert_eq!(
            left_out(state),
            0,
            "a group's rows stand for no one file, so nothing there is left out"
        );

        state.drill_up().unwrap();
        assert_eq!(
            state.lf.clone().collect().unwrap().height(),
            3,
            "the frame that comes back still leaves the two out"
        );
        assert_eq!(
            left_out(state),
            1,
            "so the note is back with it: {:#?}",
            state.notes()
        );
    }

    /// A rollback that stops half way leaves a state that is neither the template's nor
    /// the user's, and the note then describes the half that lost.
    ///
    /// The user has no sort at all; the template brings one, on a column the files
    /// disagree on, and then fails on a column order that does not fit. Every step of
    /// the rollback used to be guarded on the one before, and the first of them
    /// collected against the template's column order and errored — so the template's
    /// sort stayed in the sidebar, the notes were built from it, and the row counter
    /// reported a frame three rows shorter than the one on screen.
    #[test]
    fn a_rollback_that_fails_early_still_puts_all_of_the_view_back() {
        use polars::prelude::{ParquetWriter, df};
        let dir = tempfile::tempdir().unwrap();
        let write = |sub: &str, mut frame: polars::prelude::DataFrame| {
            let d = dir.path().join(sub);
            std::fs::create_dir_all(&d).unwrap();
            let f = std::fs::File::create(d.join("data.parquet")).unwrap();
            ParquetWriter::new(f).finish(&mut frame).unwrap();
        };
        write(
            "date=2024-01-01",
            df!("id" => &[0i64, 1, 2], "n" => &[0i64, 1, 2]).unwrap(),
        );
        write(
            "date=2024-01-02",
            df!("id" => &[3i64, 4], "n" => &["x", "y"]).unwrap(),
        );

        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx.clone(), crate::tests::test_runtime());
        app.input_mode = InputMode::Normal;
        let opts = OpenOptions {
            hive: true,
            ..OpenOptions::default()
        };
        if let Some(next) = app.event(&AppEvent::Open(vec![dir.path().to_path_buf()], opts)) {
            let _ = tx.send(next);
        }
        super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| {
            a.data_table_state.is_some() && !a.is_busy()
        });
        app.data_table_state.as_mut().unwrap().mark_notes_seen();
        let order_before = app
            .data_table_state
            .as_ref()
            .unwrap()
            .get_column_order()
            .to_vec();

        let mut template = app
            .create_template_from_current_state(
                "sort then break".to_string(),
                None,
                template::MatchCriteria {
                    exact_path: None,
                    relative_path: None,
                    path_pattern: None,
                    filename_pattern: None,
                    schema_columns: None,
                    schema_types: None,
                },
            )
            .unwrap();
        // A sort the user never asked for, and a column order that cannot be applied.
        template.settings.sort_columns = vec!["n".to_string()];
        template.settings.column_order = vec!["no_such_column".to_string()];
        assert!(app.apply_template(&template).is_err());

        let state = app.data_table_state.as_ref().unwrap();
        assert!(
            state.view_sort_columns().is_empty(),
            "the template's sort does not survive its own failure"
        );
        assert_eq!(
            state.get_column_order(),
            order_before,
            "nor does the column order it failed on"
        );
        assert_eq!(
            state.lf.clone().collect().unwrap().height(),
            5,
            "the user's frame is whole"
        );
        assert_eq!(
            state
                .notes()
                .iter()
                .filter(|note| note.summary.contains("is not read from"))
                .count(),
            0,
            "so nothing says rows went: {:#?}",
            state.notes()
        );
        assert!(
            !state.notes_unseen(),
            "and a rollback is not news, so the accent stays where the user left it"
        );
        assert!(state.error.is_none(), "with no error left over");
    }

    /// A template whose SQL drops a column that the same template's sort names. The
    /// sorted frame cannot be built at all, so the row count errors — and reporting
    /// that as zero rows used to blank the table and return before `load_buffer`, the
    /// only other place a failure is recorded. `apply_template` decides whether to roll
    /// back by looking for an error, found none, and returned `Ok`: the user was left
    /// with a blank table wearing the template's sort, told nothing.
    #[test]
    fn a_template_whose_sort_names_a_column_its_query_removed_fails_loudly() {
        crate::tests::ensure_sample_data();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("three.csv");
        std::fs::write(&path, "id,keep,dropped\n0,a,7\n1,b,8\n2,c,9\n").unwrap();
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx.clone(), crate::tests::test_runtime());
        open(&mut app, &rx, &tx, path);

        let mut template = app
            .create_template_from_current_state(
                "sort what the query dropped".to_string(),
                None,
                template::MatchCriteria {
                    exact_path: None,
                    relative_path: None,
                    path_pattern: None,
                    filename_pattern: None,
                    schema_columns: None,
                    schema_types: None,
                },
            )
            .unwrap();
        template.settings.sql_query = Some("select id, keep from df".to_string());
        // Applied after the query, and naming the column the query just dropped.
        template.settings.sort_columns = vec!["dropped".to_string()];

        assert!(
            app.apply_template(&template).is_err(),
            "the template fails, rather than quietly leaving a blank table"
        );

        let state = app.data_table_state.as_ref().unwrap();
        assert!(
            state.view_sort_columns().is_empty(),
            "the sort it failed on does not survive"
        );
        assert!(
            state.active_sql_query.is_empty(),
            "nor does the query that dropped the column"
        );
        let names: Vec<&str> = state.schema.iter_names().map(|n| n.as_str()).collect();
        assert_eq!(names, ["id", "keep", "dropped"], "the user's frame is back");
        assert_eq!(
            state.lf.clone().collect().unwrap().height(),
            3,
            "with its rows, rather than the blank table the failure used to leave"
        );
        assert!(state.error.is_none(), "and the rollback clears the error");
    }
}

#[cfg(all(test, feature = "sql"))]
mod view_matching_tests {
    use super::chart_prepare_tests::open;
    use super::*;
    use std::sync::mpsc;

    /// A view saved on a remote dataset under a query that renames its columns
    /// records the URL and the columns as loaded: it matches the same URL as the
    /// same file, and another file with the source columns by schema.
    #[test]
    fn a_view_on_a_queried_remote_dataset_matches_by_url_and_source_columns() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tmax.csv");
        std::fs::write(
            &path,
            "ID,DATE,DATA_VALUE\nUSW1,20240101,55\nUSW1,20240102,61\n",
        )
        .unwrap();
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx.clone(), crate::tests::test_runtime());
        open(&mut app, &rx, &tx, path);
        // Views of this test's own, so no other test's saved views show in the list.
        let config = crate::config::ConfigManager::with_dir(dir.path().join("config"));
        app.template_manager = TemplateManager::new(&config).unwrap();

        // Stand in for an S3 dataset: only the path decides how a view records it.
        let url = PathBuf::from("s3://noaa-ghcn-pds/parquet/by_year/YEAR=2024/ELEMENT=TMAX/");
        app.path = Some(url.clone());
        app.data_table_state.as_mut().unwrap().sql_query(
            "SELECT DATE AS day, DATA_VALUE / 10.0 AS high_c FROM df WHERE ID = 'USW1'".to_string(),
        );
        assert!(app.data_table_state.as_ref().unwrap().error.is_none());

        app.open_save_view_form();
        assert_eq!(
            app.template_modal.exact_path_input.value(),
            url.to_string_lossy(),
            "the URL, not the working directory joined to it"
        );
        assert_eq!(
            app.template_modal.relative_path_input.value(),
            "",
            "a URL has no relative form"
        );
        app.save_view_form();
        let saved = &app.template_manager.all_templates()[0].match_criteria;
        assert_eq!(saved.exact_path.as_deref(), Some(url.as_path()));
        assert_eq!(saved.relative_path, None);
        assert_eq!(
            saved.schema_columns.as_deref(),
            Some(
                &[
                    "ID".to_string(),
                    "DATE".to_string(),
                    "DATA_VALUE".to_string()
                ][..]
            ),
            "the columns as loaded, not the query's output"
        );

        app.refresh_view_list();
        assert_eq!(
            app.template_modal.rows[0].reason,
            Some(template::MatchReason::SameFile)
        );

        // The next year's file, freshly opened: its columns are the source columns.
        let next = dir.path().join("tmax_2023.csv");
        std::fs::write(&next, "ID,DATE,DATA_VALUE\nUSW1,20230101,40\n").unwrap();
        open(&mut app, &rx, &tx, next);
        app.path = Some(PathBuf::from(
            "s3://noaa-ghcn-pds/parquet/by_year/YEAR=2023/ELEMENT=TMAX/",
        ));
        app.refresh_view_list();
        assert_eq!(
            app.template_modal.rows[0].reason,
            Some(template::MatchReason::SameColumns)
        );

        // `V` applies it there: the next year's rows under the saved query.
        app.event(&AppEvent::Key(KeyEvent::new(
            KeyCode::Char('V'),
            KeyModifiers::NONE,
        )));
        let state = app.data_table_state.as_ref().unwrap();
        assert!(state.error.is_none(), "{:?}", state.error);
        assert!(app.active_template_id.is_some(), "the view is applied");
        let names: Vec<&str> = state.schema.iter_names().map(|n| n.as_str()).collect();
        assert_eq!(names, ["day", "high_c"]);
    }
}

#[cfg(all(test, feature = "cloud"))]
mod peek_answer_tests {
    use crate::discover::{EntryKind, Holds};

    fn counted(format: &str, n: usize) -> Holds {
        Holds {
            formats: vec![(format.to_string(), n)],
            ..Default::default()
        }
    }

    /// A peek is worth a listing rebuild when its answer reaches the screen, and a
    /// rebuild reads the dataset index on the thread drawing the frame — so the test is
    /// what the answer says, not what the peek decided. The screen is the row *and* the
    /// details pane beside it, which draws the whole `holds` line.
    #[test]
    fn a_peek_is_worth_a_rebuild_when_its_answer_says_anything() {
        let worth = crate::App::peek_tells_a_row_something;

        // The claim staked before the answers arrive: nothing counted, nothing decided.
        // The only thing there is no reason to send.
        assert!(!worth(&(EntryKind::Directory, Holds::default())));

        // Only Parquet is read in place, so a prefix of twelve CSV objects stays a
        // `Directory` — and it is still `12 csv`, which is the label the row draws.
        assert!(worth(&(EntryKind::Directory, counted("csv", 12))));
        // A prefix of sub-prefixes and a writer's own files draws no label of its own,
        // but the pane has `12 directories · 3 skipped` to say, and says it on disk.
        assert!(worth(&(
            EntryKind::Directory,
            Holds {
                directories: 12,
                skipped: 3,
                ..Default::default()
            }
        )));
        // A README and two PDFs: nothing datui reads, which is itself the answer.
        assert!(worth(&(
            EntryKind::Directory,
            Holds {
                not_read: 3,
                ..Default::default()
            }
        )));
        // A listing cut short says so. Nothing draws it on this route today — see
        // the note on the function — but `is_empty` is one definition and this is
        // what it says.
        assert!(worth(&(
            EntryKind::Directory,
            Holds {
                truncated: true,
                ..Default::default()
            }
        )));

        // And the kinds that decide something say it whether they counted or not — a
        // partitioned lake table has no data file at its root, so its `Holds` is empty
        // and the kind is the whole of the answer.
        for kind in [
            EntryKind::Hive,
            EntryKind::Delta,
            EntryKind::Iceberg,
            EntryKind::Hudi,
        ] {
            assert!(worth(&(kind, Holds::default())), "{kind:?} decides the row");
        }
        assert!(worth(&(EntryKind::MultiFile, counted("parquet", 40))));
    }
}

#[cfg(test)]
mod text_input_flows;

#[cfg(test)]
pub mod tests {
    use std::path::Path;
    use std::process::Command;
    use std::sync::Once;

    static INIT: Once = Once::new();

    #[cfg(feature = "cloud")]
    mod cloud_recent_facts {
        use crate::cloud_hive::{DatasetFile, FileFooter};
        use crate::discover::EntryKind;
        use polars::prelude::{DataType, Field, Schema};
        use std::sync::Arc;

        fn file(key: &str, size: u64, stamp: u64) -> DatasetFile {
            DatasetFile {
                key: key.to_string(),
                size,
                stamp,
                etag: None,
            }
        }

        fn footer(rows: &[usize]) -> Option<FileFooter> {
            let schema = Schema::from_iter([
                Field::new("id".into(), DataType::Int64),
                Field::new("amount".into(), DataType::Float64),
            ]);
            Some(FileFooter {
                schema: Arc::new(schema),
                row_group_rows: rows.to_vec(),
                row_group_bytes: rows.iter().map(|r| r * 8).collect(),
            })
        }

        /// What the home screen shows for a recent opened from a bucket comes from the
        /// record the open wrote: rows, columns, the kind and what it holds, under the
        /// URL as opened.
        #[test]
        fn an_open_records_what_the_home_screen_will_show() {
            let files = vec![
                file("sales/year=2024/part-0.parquet", 1000, 10),
                file("sales/year=2025/part-0.parquet", 2000, 20),
            ];
            let read = vec![0, 1];
            let footers = vec![footer(&[5, 7]), footer(&[8])];
            let (path, facts) =
                crate::App::facts_from_cloud_footers("s3://bucket/sales/", &files, &read, &footers)
                    .expect("facts");
            assert_eq!(path, std::path::PathBuf::from("s3://bucket/sales/"));
            assert_eq!(facts.rows, Some(20));
            assert_eq!(facts.cols, Some(3), "the partition column counts");
            assert!(!facts.cols_sampled);
            assert_eq!(facts.kind, Some(EntryKind::Hive));
            assert_eq!(facts.holds.formats, vec![("parquet".to_string(), 2)]);
            assert_eq!(facts.size, 3000);
            assert_eq!(facts.mtime, 20);
            assert_eq!(facts.classified_by, crate::discover::CLASSIFIER_VERSION);
            assert!(facts.columns.iter().any(|c| c == "amount"));

            // A flat prefix is a directory of files; one object is a file.
            let flat = vec![file("sales/a.parquet", 1, 1), file("sales/b.parquet", 1, 1)];
            let (_, facts) = crate::App::facts_from_cloud_footers(
                "s3://bucket/sales/",
                &flat,
                &[0, 1],
                &[footer(&[1]), footer(&[1])],
            )
            .unwrap();
            assert_eq!(facts.kind, Some(EntryKind::MultiFile));
            let one = vec![file("sales/a.parquet", 1, 1)];
            let (_, facts) = crate::App::facts_from_cloud_footers(
                "s3://bucket/sales/a.parquet",
                &one,
                &[0],
                &[footer(&[4])],
            )
            .unwrap();
            assert_eq!(facts.kind, Some(EntryKind::File));
            assert!(facts.holds.is_empty());
            assert_eq!(facts.rows, Some(4));
        }

        /// `--hive` on a prefix with no trailing slash lists the prefix's files and is a
        /// directory; on a single object it lists that object and is a file.
        #[test]
        fn a_prefix_without_its_slash_is_still_a_directory() {
            let files = vec![file("sales/a.parquet", 1, 1), file("sales/b.parquet", 1, 1)];
            let (_, facts) = crate::App::facts_from_cloud_footers(
                "s3://bucket/sales",
                &files,
                &[0, 1],
                &[footer(&[1]), footer(&[1])],
            )
            .unwrap();
            assert_eq!(facts.kind, Some(EntryKind::MultiFile));
            let one = vec![file("sales/a.parquet", 1, 1)];
            let (_, facts) = crate::App::facts_from_cloud_footers(
                "s3://bucket/sales/a.parquet",
                &one,
                &[0],
                &[footer(&[1])],
            )
            .unwrap();
            assert_eq!(facts.kind, Some(EntryKind::File));
        }

        /// A sampled read does not replace a whole one: the shape cache forgets a
        /// dataset long before the index does, and a reopen reads a sample first.
        #[test]
        fn a_sample_never_replaces_a_whole_record() {
            let whole = crate::cache::DatasetFacts {
                rows: Some(20),
                ..Default::default()
            };
            let sample = crate::cache::DatasetFacts {
                rows: None,
                cols_sampled: true,
                ..Default::default()
            };
            assert!(crate::App::facts_worth_recording(None, &sample));
            assert!(crate::App::facts_worth_recording(Some(&sample), &sample));
            assert!(crate::App::facts_worth_recording(Some(&sample), &whole));
            assert!(crate::App::facts_worth_recording(Some(&whole), &whole));
            assert!(!crate::App::facts_worth_recording(Some(&whole), &sample));
        }

        /// One object opened from a bucket is recorded from the footer the open read:
        /// its rows and columns, no size, which the row then does not show as zero.
        #[test]
        fn one_object_is_recorded_from_its_footer() {
            let tmp = tempfile::tempdir().unwrap();
            let cache = crate::cache::CacheManager::with_dir(tmp.path().to_path_buf());
            let schema = Schema::from_iter([Field::new("id".into(), DataType::Int64)]);
            let footer = crate::cloud_hive::ParquetFooter {
                schema: Arc::new(schema),
                row_group_rows: vec![3, 4],
                column_bytes_per_row: Vec::new(),
            };
            crate::App::record_cloud_object_facts(Some(&cache), "s3://bucket/x.parquet", &footer);
            let known = cache.load_dataset_facts();
            let facts = known
                .get(std::path::Path::new("s3://bucket/x.parquet"))
                .expect("recorded");
            assert_eq!(facts.rows, Some(7));
            assert_eq!(facts.cols, Some(1));
            assert_eq!(facts.kind, Some(EntryKind::File));
            assert_eq!(facts.size, 0);
            assert!(
                facts.mtime > 0,
                "dated, so the index does not evict it first"
            );
        }

        /// A sampled read knows the columns and not the rows, and says the width is a
        /// floor.
        #[test]
        fn a_sampled_read_records_columns_but_no_row_count() {
            let files: Vec<DatasetFile> = (0..5)
                .map(|i| file(&format!("x/p{i}.parquet"), 10, 1))
                .collect();
            let (_, facts) = crate::App::facts_from_cloud_footers(
                "s3://bucket/x/",
                &files,
                &[0, 4],
                &[footer(&[1]), footer(&[1])],
            )
            .unwrap();
            assert_eq!(facts.rows, None);
            assert_eq!(facts.cols, Some(2));
            assert!(facts.cols_sampled);
        }
    }

    /// Ensures that sample data files are generated before tests run.
    /// This function uses `std::sync::Once` to ensure it only runs once,
    /// even if called from multiple tests.
    pub fn ensure_sample_data() {
        INIT.call_once(|| {
            // When the lib is in crates/datui-lib, repo root is CARGO_MANIFEST_DIR/../..
            let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
            let sample_data_dir = repo_root.join("tests/sample-data");

            // Check if key files exist to determine if we need to generate data
            // We check for a few representative files that should always be generated
            let key_files = [
                "people.parquet",
                "sales.parquet",
                "large_dataset.parquet",
                "empty.parquet",
                "pivot_long.parquet",
                "melt_wide.parquet",
                "infer_schema_length_data.csv",
            ];

            let needs_generation = !sample_data_dir.exists()
                || key_files
                    .iter()
                    .any(|file| !sample_data_dir.join(file).exists());

            if needs_generation {
                // Get the path to the Python script (at repo root)
                let script_path = repo_root.join("scripts/generate_sample_data.py");
                if !script_path.exists() {
                    panic!(
                        "Sample data generation script not found at: {}. \
                        Please ensure you're running tests from the repository root.",
                        script_path.display()
                    );
                }

                // Try to find Python (python3 or python)
                let python_cmd = if Command::new("python3").arg("--version").output().is_ok() {
                    "python3"
                } else if Command::new("python").arg("--version").output().is_ok() {
                    "python"
                } else {
                    panic!(
                        "Python not found. Please install Python 3 to generate test data. \
                        The script requires: polars>=0.20.0 and numpy>=1.24.0"
                    );
                };

                // Run the generation script
                let output = Command::new(python_cmd)
                    .arg(script_path)
                    .output()
                    .unwrap_or_else(|e| {
                        panic!(
                            "Failed to run sample data generation script: {}. \
                            Make sure Python is installed and the script is executable.",
                            e
                        );
                    });

                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    panic!(
                        "Sample data generation failed!\n\
                        Exit code: {:?}\n\
                        stdout:\n{}\n\
                        stderr:\n{}",
                        output.status.code(),
                        stdout,
                        stderr
                    );
                }
            }
        });
    }

    /// Returns a tokio runtime handle for use in tests.
    ///
    /// Also points the cache at a directory of the test run's own. Every `App` a test
    /// builds takes this handle, and an `App` that opens a file records it in recents;
    /// without the redirect those temp-dir fixtures land in the developer's own home
    /// screen as dead roots.
    pub fn test_runtime() -> tokio::runtime::Handle {
        crate::text_input_flows::isolate_cache();
        static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
        RT.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .expect("test tokio runtime")
        })
        .handle()
        .clone()
    }

    /// The footer read, the size probe and a download go through the store Polars
    /// scans with, found in its cache by bucket and options, so the same object
    /// yields the same store.
    #[cfg(feature = "cloud")]
    #[test]
    fn one_object_store_serves_the_footer_the_probe_and_the_scan() {
        let cloud = crate::config::CloudConfig {
            s3_endpoint_url: Some("http://127.0.0.1:1".to_string()),
            s3_region: Some("us-east-1".to_string()),
            s3_access_key_id: Some("testing".to_string()),
            s3_secret_access_key: Some("testing".to_string()),
            ..Default::default()
        };
        let rt = test_runtime();
        let path = std::path::Path::new("s3://bucket/obj.parquet");
        let (url, _, store) = super::App::cloud_store_for(path, &cloud, &rt).expect("a store");
        assert_eq!(url, "s3://bucket/obj.parquet");
        let (_, _, again) = super::App::cloud_store_for(path, &cloud, &rt).expect("the same store");
        assert!(
            std::sync::Arc::ptr_eq(&store, &again),
            "built once, served twice"
        );
        let (_, _, probe) =
            super::App::cloud_store_for(std::path::Path::new("s3://bucket/"), &cloud, &rt)
                .expect("the bucket's store");
        assert!(
            std::sync::Arc::ptr_eq(&store, &probe),
            "the probe shares it"
        );
    }

    /// Quitting while a bucket listing was still out used to panic on the listing's
    /// thread. A timer stands in for the request's timeout, which is what tripped.
    #[cfg(feature = "cloud")]
    #[test]
    fn a_runtime_shut_down_under_a_waiting_thread_does_not_panic_it() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("tokio runtime");
        let handle = rt.handle().clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            super::wait_on_runtime(&handle, async move {
                let _ = started_tx.send(());
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            })
        });
        started_rx.recv().expect("future started");
        rt.shutdown_background();
        let outcome = waiter.join().expect("the waiting thread must not panic");
        assert!(outcome.is_none());
    }

    /// Path to the tests/sample-data directory (at repo root). Call `ensure_sample_data()` first if needed.
    pub fn sample_data_dir() -> std::path::PathBuf {
        ensure_sample_data();
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("tests/sample-data")
    }

    /// End pressed while the footers are still coming waits for them, then jumps.
    ///
    /// The pass is already reading every footer and those footers hold the count, so a
    /// count started here would read all of them a second time. Worse, the join takes a
    /// fresh `len_generation` on its way past, so the answer would come back to a
    /// question nothing could match it to and the jump would never happen — the user
    /// would be left with "Counting rows to find the end..." and no end.
    #[test]
    fn end_pressed_while_the_footers_are_coming_jumps_when_they_land() {
        use crate::widgets::datatable::{DataTableState, FootersFound, RemoteFiles};
        use crate::{App, AppEvent, OpenOptions};
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        use polars::prelude::*;
        use std::sync::Arc;

        let rows = || df!("id" => (0..100i64).collect::<Vec<_>>()).unwrap().lazy();
        let dataset_of = |lf: LazyFrame| {
            let mut lf = lf;
            let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
            let footer = crate::schema_union::FileSchema {
                schema,
                rows: 100,
                file_bytes: 0,
                row_group_bytes: Vec::new(),
            };
            crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
        };

        let mut state = DataTableState::from_schema_and_lazyframe(
            dataset_of(rows()).schema.clone(),
            rows(),
            &OpenOptions::default(),
            None,
        )
        .unwrap();
        state.set_remote_source();
        state.set_remote_files(RemoteFiles {
            urls: Arc::new(vec!["one".to_string()]),
            scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
            count: Arc::new(|| Ok(vec![vec![100]])),
            offsets: None,
        });
        state.visible_rows = 10;
        state.set_footers_pending(Arc::new(move |_| {
            Some(FootersFound {
                dataset: dataset_of(rows()),
                lf: rows(),
                file_rows: vec![100],
                files: vec!["one".to_string()],
                row_groups: vec![vec![100]],
                remote: Some(crate::widgets::datatable::RemoteRead {
                    urls: vec!["one".to_string()],
                    scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
                    count: Arc::new(|| Ok(vec![vec![100]])),
                }),
            })
        }));

        let (tx, rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.load_active = true;
        app.apply_schema_ready(state, None, &OpenOptions::default(), None);

        // End, while the footers are still on their way.
        let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
        assert!(
            app.len_count_inflight.is_none(),
            "no second pass over the footers already being read"
        );

        // They land.
        let reported = loop {
            let event = rx
                .recv_timeout(std::time::Duration::from_secs(10))
                .expect("the pass reports back");
            if matches!(event, AppEvent::BackgroundFootersJoined { .. }) {
                break event;
            }
        };
        let _ = app.handle(&reported);
        // The jump the key asked for is queued behind the join, as a jump always is.
        while let Ok(event) = rx.try_recv() {
            let _ = app.handle(&event);
        }

        let state = app.data_table_state.as_ref().unwrap();
        assert_eq!(state.num_rows_if_valid(), Some(100), "counted by the pass");
        assert!(
            state.start_row > 0,
            "and the end is where the view went, rather than the key being swallowed"
        );
        assert_ne!(
            app.status_message.as_deref(),
            Some("Counting rows to find the end..."),
            "with nothing left saying it is counting rows that have been counted"
        );
        assert!(
            app.end_when_the_footers_land.is_none(),
            "and the key is spent, not left waiting on the next dataset"
        );
    }

    /// Every key the busy classifier lets through must read nothing.
    ///
    /// `key_acts_while_busy` admits column scroll and help while other work is in
    /// flight, and `App::handle` runs them inline on the thread that draws and reads
    /// the keyboard. The premise is that none of them touches the source. Column
    /// scroll broke it: it called `collect`, which counts the rows when the count has
    /// not landed, and on a staged-open cloud hive that count is a metadata read per
    /// object — a freeze no keystroke can interrupt.
    ///
    /// The admitted set is *asked for* rather than written down again, so a key added
    /// to the classifier is covered by this the moment it is added. Reverting
    /// `scroll_right`/`scroll_left` to `self.collect()` fails it, and so does admitting
    /// a key that collects — `Char('R')`, say.
    #[test]
    fn a_key_that_acts_while_busy_reads_nothing() {
        use crate::widgets::datatable::DataTableState;
        use crate::{App, OpenOptions};
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        use polars::prelude::*;
        use std::sync::Arc;

        let rows = || {
            df!(
                "a" => (0..100i64).collect::<Vec<_>>(),
                "b" => (0..100i64).collect::<Vec<_>>(),
                "c" => (0..100i64).collect::<Vec<_>>(),
                "d" => (0..100i64).collect::<Vec<_>>(),
            )
            .unwrap()
            .lazy()
        };
        let mut lf = rows();
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());

        // Every key the table view can see, so the filter below is the classifier's
        // answer rather than a copy of its arms.
        let mut candidates: Vec<KeyCode> = (b'a'..=b'z')
            .chain(b'A'..=b'Z')
            .map(|c| KeyCode::Char(c as char))
            .collect();
        candidates.extend((1..=12).map(KeyCode::F));
        candidates.extend([
            KeyCode::Left,
            KeyCode::Right,
            KeyCode::Up,
            KeyCode::Down,
            KeyCode::Home,
            KeyCode::End,
            KeyCode::PageUp,
            KeyCode::PageDown,
            KeyCode::Enter,
            KeyCode::Esc,
            KeyCode::Tab,
            KeyCode::Backspace,
        ]);

        let staged = || {
            let mut state = DataTableState::from_schema_and_lazyframe(
                schema.clone(),
                rows(),
                &OpenOptions::default(),
                None,
            )
            .unwrap();
            state.visible_rows = 10;
            state.visible_termcols = 1;
            // A page on screen, then the count dropped: what a staged open looks like
            // while its footers are still being read. With a buffer in hand the scroll
            // keys do their real work, so this asks whether that work counts.
            state.set_num_rows(100);
            state.collect();
            state.scroll_right();
            state.invalidate_num_rows();
            state
        };

        let mut admitted = 0;
        for code in candidates {
            let key = KeyEvent::new(code, KeyModifiers::NONE);
            let (tx, _rx) = std::sync::mpsc::channel();
            let mut app = App::new(tx, crate::tests::test_runtime());
            app.load_active = true;
            app.apply_schema_ready(staged(), None, &OpenOptions::default(), None);
            if !app.key_acts_while_busy(&key) {
                continue;
            }
            admitted += 1;
            assert_eq!(
                app.data_table_state
                    .as_ref()
                    .and_then(|s| s.num_rows_if_valid()),
                None,
                "{code:?}: the staged open should start without a count"
            );

            // The event the key returns is part of what the key does: `Char('R')`
            // acts by handing back `AppEvent::Reset`, and dropping it here would let
            // an admitted key that collects through the check.
            if let Some(next) = app.key(&key) {
                let _ = app.handle(&next);
            }

            assert_eq!(
                app.data_table_state
                    .as_ref()
                    .and_then(|s| s.num_rows_if_valid()),
                None,
                "{code:?} acts while busy, so it must not count the rows"
            );
        }
        assert!(
            admitted >= 6,
            "the classifier should admit the view keys; it admitted {admitted}"
        );
    }

    /// End pressed at one dataset does not move the view of the next.
    ///
    /// Abandoning a load cancels nothing, so the prefix the user pressed End on goes on
    /// reading its footers after they have left it. Without the dataset's name on it,
    /// the key would be spent on whatever is on screen when they land — a directory the
    /// user has only just opened jumping to its end on its own.
    #[test]
    fn end_pressed_at_one_dataset_does_not_move_the_next() {
        use crate::widgets::datatable::{DataTableState, FootersFound, RemoteFiles};
        use crate::{App, OpenOptions};
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        use polars::prelude::*;
        use std::sync::Arc;

        let rows = || df!("id" => (0..100i64).collect::<Vec<_>>()).unwrap().lazy();
        let dataset_of = |lf: LazyFrame| {
            let mut lf = lf;
            let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
            let footer = crate::schema_union::FileSchema {
                schema,
                rows: 100,
                file_bytes: 0,
                row_group_bytes: Vec::new(),
            };
            crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
        };
        let staged = move || {
            let mut state = DataTableState::from_schema_and_lazyframe(
                dataset_of(rows()).schema.clone(),
                rows(),
                &OpenOptions::default(),
                None,
            )
            .unwrap();
            state.set_remote_source();
            state.set_remote_files(RemoteFiles {
                urls: Arc::new(vec!["one".to_string()]),
                scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
                count: Arc::new(|| Ok(vec![vec![100]])),
                offsets: None,
            });
            state.visible_rows = 10;
            state.set_footers_pending(Arc::new(move |_| {
                Some(FootersFound {
                    dataset: dataset_of(rows()),
                    lf: rows(),
                    file_rows: vec![100],
                    files: vec!["one".to_string()],
                    row_groups: vec![vec![100]],
                    remote: Some(crate::widgets::datatable::RemoteRead {
                        urls: vec!["one".to_string()],
                        scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
                        count: Arc::new(|| Ok(vec![vec![100]])),
                    }),
                })
            }));
            state
        };

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.load_active = true;
        app.apply_schema_ready(staged(), None, &OpenOptions::default(), None);
        let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
        assert!(
            app.end_when_the_footers_land.is_some(),
            "the key is waiting on this dataset's footers"
        );

        // The user goes elsewhere before they land.
        app.load_active = true;
        app.apply_schema_ready(staged(), None, &OpenOptions::default(), None);
        assert!(
            app.end_when_the_footers_land.is_none(),
            "and does not take the key with them"
        );
        assert_eq!(
            app.data_table_state.as_ref().unwrap().start_row,
            0,
            "the directory they opened is where they left it, at the top"
        );
    }

    /// Once the count is known, nothing is still counting.
    ///
    /// A staged open declines the standalone row count, because the pass reading the
    /// footers is bringing it. The marker that says a count is running must not be set
    /// for a count that was never started: it is cleared only by a count coming back,
    /// so it would stay set for the rest of the session — leaving a spinner where the
    /// row count goes, the event loop redrawing for it, and `End` waiting on a count
    /// that is not running.
    #[test]
    fn a_staged_open_does_not_leave_a_count_running_that_never_ran() {
        use crate::widgets::datatable::{DataTableState, FootersFound, RemoteFiles};
        use crate::{App, AppEvent, OpenOptions};
        use polars::prelude::*;
        use std::sync::Arc;

        let narrow = || df!("id" => (0..100i64).collect::<Vec<_>>()).unwrap().lazy();
        let wide = || {
            df!("id" => (0..100i64).collect::<Vec<_>>(), "oops" => vec!["a"; 100])
                .unwrap()
                .lazy()
        };
        let dataset_of = |lf: LazyFrame| {
            let mut lf = lf;
            let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
            let footer = crate::schema_union::FileSchema {
                schema,
                rows: 100,
                file_bytes: 0,
                row_group_bytes: Vec::new(),
            };
            crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
        };

        let mut state = DataTableState::from_schema_and_lazyframe(
            dataset_of(narrow()).schema.clone(),
            narrow(),
            &OpenOptions::default(),
            None,
        )
        .unwrap();
        state.set_remote_source();
        state.set_remote_files(RemoteFiles {
            urls: Arc::new(vec!["one".to_string()]),
            scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(narrow())),
            count: Arc::new(|| Ok(vec![vec![100]])),
            offsets: None,
        });
        state.visible_rows = 10;
        state.set_footers_pending(Arc::new(move |_| {
            Some(FootersFound {
                dataset: dataset_of(wide()),
                lf: wide(),
                file_rows: vec![100],
                files: vec!["one".to_string()],
                row_groups: vec![vec![100]],
                remote: Some(crate::widgets::datatable::RemoteRead {
                    urls: vec!["one".to_string()],
                    scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(wide())),
                    count: Arc::new(|| Ok(vec![vec![100]])),
                }),
            })
        }));

        let (tx, rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.load_active = true;
        app.apply_schema_ready(state, None, &OpenOptions::default(), None);
        app.spawn_async_collect(App::LOADING_BUFFER);

        // Two answers are in flight here — the pass's and the collect's — and either
        // can reach the queue first. Taking whatever arrives first and calling it the
        // pass's is a race: when the collect wins, the count the pass carries has not
        // been applied yet and the assert below reads `None`. It loses that race about
        // once in a few hundred runs on a loaded machine, which is every so often on
        // CI. So take events until the pass's own has been handled.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let event = rx
                .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
                .expect("the pass reports back");
            let is_the_pass = matches!(event, AppEvent::BackgroundFootersJoined { .. });
            let _ = app.handle(&event);
            if is_the_pass {
                break;
            }
        }

        assert_eq!(
            app.data_table_state.as_ref().unwrap().num_rows_if_valid(),
            Some(100),
            "the pass brought the count with it"
        );
        assert!(
            app.len_count_inflight.is_none(),
            "so nothing is still counting, and the row count is a number rather than a \
             spinner for the rest of the session"
        );
    }

    /// A pass that cannot read the footers leaves the dataset working, not waiting.
    ///
    /// While one is pending the dataset declines to count itself, because counting
    /// means reading the same footers the pass is already fetching. If a failed pass
    /// said nothing, that would be permanent: no exact row count for the rest of the
    /// session, no windowed reads, and nothing on screen to say why.
    #[test]
    fn a_pass_that_cannot_read_the_footers_stops_the_dataset_waiting_for_it() {
        use crate::widgets::datatable::DataTableState;
        use crate::{App, AppEvent, OpenOptions};
        use polars::prelude::*;
        use std::sync::Arc;

        let frame = || df!("id" => &[1i64]).unwrap().lazy();
        let mut lf = frame();
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());

        let (tx, rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        let mut state = DataTableState::from_schema_and_lazyframe(
            schema,
            frame(),
            &OpenOptions::default(),
            None,
        )
        .unwrap();
        // The network was there for the open's two footers and gone for the rest.
        state.set_footers_pending(Arc::new(|_progress| None));
        app.load_active = true;
        app.apply_schema_ready(state, None, &OpenOptions::default(), None);

        let reported = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("a pass that failed still says so");
        assert!(
            matches!(reported, AppEvent::BackgroundFootersJoined { .. }),
            "and says it the same way a pass that succeeded does"
        );
        let _ = app.handle(&reported);

        assert!(
            app.data_table_state
                .as_ref()
                .unwrap()
                .footers_pending()
                .is_none(),
            "the dataset is not left waiting for footers that are not coming"
        );
    }

    /// A count not yet taken is not printed as the total.
    ///
    /// The control bar takes its number straight from the field, so the only thing
    /// between a user and `Rows: 70` on a prefix of six thousand files is the pending
    /// flag. The integration test beside this one has a dataset whose count is real and
    /// asserts it is shown; this is the direction that goes wrong.
    #[test]
    fn a_count_not_yet_taken_is_not_printed_as_the_total() {
        use crate::widgets::datatable::DataTableState;
        use crate::{App, OpenOptions};
        use polars::prelude::*;
        use ratatui::buffer::Buffer;
        use ratatui::layout::Rect;
        use ratatui::widgets::Widget;
        use std::sync::Arc;

        let rows = || df!("id" => (0..70i64).collect::<Vec<_>>()).unwrap().lazy();
        let mut lf = rows();
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let mut state = DataTableState::from_schema_and_lazyframe(
            schema,
            rows(),
            &OpenOptions::default(),
            None,
        )
        .unwrap();
        // As a staged open leaves it: the number it holds is as far as the buffer
        // reached, a pass is still out, and no count has been taken.
        // The field, not `set_num_rows`, which would mark it as a count that had been
        // taken — the state this reproduces is a provisional left by a short read.
        state.num_rows = 70;
        state.set_footers_pending(Arc::new(|_| None));
        assert!(
            state.counts_itself_later(),
            "the fixture is a dataset whose count is still coming"
        );

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.load_active = true;
        app.apply_schema_ready(state, None, &OpenOptions::default(), None);
        app.busy = false;

        let area = Rect::new(0, 0, 100, 24);
        let mut buf = Buffer::empty(area);
        (&mut app).render(area, &mut buf);
        let bar: String = (0..area.width)
            .map(|x| buf[(x, area.height - 1)].symbol().to_string())
            .collect();
        assert!(
            !bar.contains("70 rows"),
            "a partial is not a total: {bar:?}"
        );
        // And the spinner standing in for it turns. Nothing is `busy` and no count is
        // in flight, which is all the run loop used to ask.
        assert!(app.something_is_spinning());
    }

    /// A pass that brings no count still leaves rows on screen.
    ///
    /// When the pass samples past `MAX_FOOTER_READS`, or a footer fails on the second
    /// read, it comes back with no row groups — so there is no count, and an End waiting
    /// on it defers to the ordinary one instead of jumping. The join has already dropped
    /// the buffer by then, so if the jump is taken to have read the page, nothing reads
    /// it: a table with no rows in it until the next keypress.
    #[test]
    fn a_pass_that_brings_no_count_still_leaves_rows_on_screen() {
        use crate::widgets::datatable::{DataTableState, FootersFound, RemoteFiles};
        use crate::{App, AppEvent, OpenOptions};
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        use polars::prelude::*;
        use std::sync::Arc;

        let rows = || df!("id" => (0..100i64).collect::<Vec<_>>()).unwrap().lazy();
        let wider = || {
            df!("id" => (0..100i64).collect::<Vec<_>>(), "oops" => vec!["a"; 100])
                .unwrap()
                .lazy()
        };
        let dataset_of = |lf: LazyFrame| {
            let mut lf = lf;
            let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
            let footer = crate::schema_union::FileSchema {
                schema,
                rows: 100,
                file_bytes: 0,
                row_group_bytes: Vec::new(),
            };
            crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
        };

        let mut state = DataTableState::from_schema_and_lazyframe(
            dataset_of(rows()).schema.clone(),
            rows(),
            &OpenOptions::default(),
            None,
        )
        .unwrap();
        state.set_remote_source();
        state.set_remote_files(RemoteFiles {
            urls: Arc::new(vec!["one".to_string()]),
            scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
            count: Arc::new(|| Ok(vec![vec![100]])),
            offsets: None,
        });
        state.visible_rows = 10;
        // No row groups: a dataset sampled past the footer limit, or one whose footer
        // would not parse the second time.
        state.set_footers_pending(Arc::new(move |_| {
            Some(FootersFound {
                dataset: dataset_of(wider()),
                lf: wider(),
                file_rows: Vec::new(),
                files: Vec::new(),
                row_groups: Vec::new(),
                remote: None,
            })
        }));

        let (tx, rx) = std::sync::mpsc::channel();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let mut app = App::new(tx, runtime.handle().clone());
        app.load_active = true;
        app.apply_schema_ready(state, None, &OpenOptions::default(), None);
        let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));

        let reported = loop {
            let event = rx
                .recv_timeout(std::time::Duration::from_secs(10))
                .expect("the pass reports back");
            if matches!(event, AppEvent::BackgroundFootersJoined { .. }) {
                break event;
            }
        };
        let _ = app.handle(&reported);

        assert!(
            app.collect_inflight.is_some(),
            "the join dropped the buffer, so something has to read it back"
        );
    }

    /// End on a sorted staged dataset waits for the pass, as it does on a plain one.
    ///
    /// A sort is rebuilt over the joined scan, so the join lands underneath it and takes
    /// a fresh `len_generation` on its way past. A count started before that comes back
    /// answering a question nothing can match it to: the jump never happens, the status
    /// line goes on saying it is counting, and `end_after_count` is stranded at a dead
    /// generation — where a later failure for any other count prints "Could not count
    /// the rows to find the end" about something the user never asked for.
    #[test]
    fn end_on_a_sorted_dataset_still_reading_its_footers_waits_for_the_pass() {
        use crate::widgets::datatable::{DataTableState, RemoteFiles};
        use crate::{App, OpenOptions};
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        use polars::prelude::*;
        use std::sync::Arc;

        let rows = || df!("id" => (0..100i64).collect::<Vec<_>>()).unwrap().lazy();
        let mut lf = rows();
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let mut state = DataTableState::from_schema_and_lazyframe(
            schema,
            rows(),
            &OpenOptions::default(),
            None,
        )
        .unwrap();
        state.set_remote_source();
        state.set_remote_files(RemoteFiles {
            urls: Arc::new(vec!["one".to_string()]),
            scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
            count: Arc::new(|| Ok(vec![vec![100]])),
            offsets: None,
        });
        state.visible_rows = 10;
        state.set_footers_pending(Arc::new(|_| None));

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.load_active = true;
        app.apply_schema_ready(state, None, &OpenOptions::default(), None);

        // Sorted — which is one of the things the staging exists to let you do while
        // the footers read — and then End.
        let state = app.data_table_state.as_mut().unwrap();
        state.defer_collect = true;
        state.sort(vec!["id".to_string()], false);
        state.defer_collect = false;
        assert!(
            state.scan_is_the_root(),
            "a sort is rebuilt over whatever the root becomes, so the join lands under it"
        );

        let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
        assert_eq!(
            app.end_when_the_footers_land,
            Some(app.dataset_generation),
            "so End waits for the pass rather than starting a count the join will orphan"
        );
        assert!(
            app.end_after_count.is_none(),
            "and nothing is left waiting on a count that will never be matched"
        );
    }

    /// A count the join orphaned does not strand End, nor speak for a later count.
    ///
    /// End on a query over a staged dataset takes the ordinary count — the pass is
    /// bringing the *dataset's* count, which is not the query's. But the pass's columns
    /// are held while the query is up and go in the moment the user leaves it, and that
    /// join takes a fresh `len_generation` past the count already running. What comes
    /// back then answers a frame that is gone.
    ///
    /// Both halves of that were wrong. The flag was cleared only on the matching branch,
    /// so it sat on a dead generation for the rest of the session — the view never moved
    /// and nothing was said. And `BackgroundLenFailed` took the flag without checking
    /// whose count had failed, so the next count to fail for any reason printed "Could
    /// not count the rows to find the end" about a key pressed on a different frame.
    #[test]
    fn a_count_the_join_orphaned_does_not_strand_end_or_speak_for_a_later_one() {
        use crate::widgets::datatable::{DataTableState, FootersFound, RemoteFiles};
        use crate::{App, AppEvent, OpenOptions};
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        use polars::prelude::*;
        use std::sync::Arc;

        let rows = || df!("id" => (0..100i64).collect::<Vec<_>>()).unwrap().lazy();
        let wide = || {
            df!("id" => (0..100i64).collect::<Vec<_>>(), "extra" => vec!["a"; 100])
                .unwrap()
                .lazy()
        };
        let dataset_of = |lf: LazyFrame| {
            let mut lf = lf;
            let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
            let footer = crate::schema_union::FileSchema {
                schema,
                rows: 100,
                file_bytes: 0,
                row_group_bytes: Vec::new(),
            };
            crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
        };
        let mut lf = rows();
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let mut state = DataTableState::from_schema_and_lazyframe(
            schema,
            rows(),
            &OpenOptions::default(),
            None,
        )
        .unwrap();
        state.set_remote_source();
        state.set_remote_files(RemoteFiles {
            urls: Arc::new(vec!["one".to_string()]),
            scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
            count: Arc::new(|| Ok(vec![vec![100]])),
            offsets: None,
        });
        state.visible_rows = 10;
        state.set_footers_pending(Arc::new(move |_| {
            Some(FootersFound {
                dataset: dataset_of(wide()),
                lf: wide(),
                file_rows: vec![100],
                files: vec!["one".to_string()],
                row_groups: vec![vec![100]],
                remote: Some(crate::widgets::datatable::RemoteRead {
                    urls: vec!["one".to_string()],
                    scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(wide())),
                    count: Arc::new(|| Ok(vec![vec![100]])),
                }),
            })
        }));

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.load_active = true;
        app.apply_schema_ready(state, None, &OpenOptions::default(), None);

        // A question of the dataset, whose answer has a count of its own.
        let state = app.data_table_state.as_mut().unwrap();
        state.defer_collect = true;
        state.query("select doubled: id * 2".to_string());
        state.defer_collect = false;
        let orphaned = app.data_table_state.as_ref().unwrap().len_generation();

        // End, which takes that count rather than waiting for the pass.
        let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
        assert_eq!(
            app.end_after_count,
            Some(orphaned),
            "the jump is waiting on the query's own count"
        );

        // The pass lands while the query is up, so its columns are held.
        let live = app.dataset_generation;
        let found = app
            .data_table_state
            .as_ref()
            .and_then(|state| state.footers_pending())
            .and_then(|pass| pass(&app.footer_progress));
        App::record_footers(&app.pending_footers_result, live, found);
        let _ = app.handle(&AppEvent::BackgroundFootersJoined { generation: live });
        assert!(
            app.footers_held.is_some(),
            "held rather than joined, because a query is the root"
        );

        // The user leaves the query, and the join goes in underneath the count.
        let state = app.data_table_state.as_mut().unwrap();
        state.defer_collect = true;
        state.query(String::new());
        state.defer_collect = false;
        let _ = app.handle(&AppEvent::Update);
        let joined = app.data_table_state.as_ref().unwrap().len_generation();
        assert_ne!(
            joined, orphaned,
            "the join took a fresh generation past the count that was already running"
        );

        // And the count comes back, answering a frame that is gone.
        let before = app.data_table_state.as_ref().unwrap().start_row;
        let next = app.event(&AppEvent::BackgroundLenReady {
            len_generation: orphaned,
            num_rows: 100,
            file_row_groups: None,
        });
        assert_eq!(
            app.end_after_count, None,
            "the jump is not left waiting on a generation nothing will ever match"
        );
        assert_ne!(
            app.status_message.as_deref(),
            Some(App::COUNTING_FOR_END),
            "and the line does not go on saying it is counting for an end nobody awaits"
        );

        // Retired, not re-issued: nothing jumps on the strength of the stale answer.
        let mut follow = next;
        while let Some(event) = follow {
            follow = app.event(&event);
        }
        assert_eq!(
            app.data_table_state.as_ref().unwrap().start_row,
            before,
            "and the view stays where it is rather than moving on a stale answer"
        );
    }

    /// A count that failed for a frame that is gone does not answer for the End on this
    /// one.
    ///
    /// Counts for two frames can be in flight at once — a join or a query takes a fresh
    /// `len_generation` without stopping the count already running — so a failure
    /// arriving is not necessarily the failure of the count End is waiting on.
    /// `BackgroundLenFailed` took the flag without looking at whose count had failed:
    /// the older one failing dropped the live End on the floor and printed "Could not
    /// count the rows to find the end" about it, while the count that End was actually
    /// waiting on was still running and about to succeed.
    #[test]
    fn a_count_that_failed_for_another_frame_does_not_answer_for_this_end() {
        use crate::widgets::datatable::{DataTableState, RemoteFiles};
        use crate::{App, AppEvent, OpenOptions};
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        use polars::prelude::*;
        use std::sync::Arc;

        let rows = || df!("id" => (0..100i64).collect::<Vec<_>>()).unwrap().lazy();
        let mut lf = rows();
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let mut state = DataTableState::from_schema_and_lazyframe(
            schema,
            rows(),
            &OpenOptions::default(),
            None,
        )
        .unwrap();
        state.set_remote_source();
        state.set_remote_files(RemoteFiles {
            urls: Arc::new(vec!["one".to_string()]),
            scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
            count: Arc::new(|| Ok(vec![vec![100]])),
            offsets: None,
        });
        state.visible_rows = 10;

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.load_active = true;
        app.apply_schema_ready(state, None, &OpenOptions::default(), None);

        // End on the frame that is here, whose count is running.
        let live = app.data_table_state.as_ref().unwrap().len_generation();
        let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
        assert_eq!(
            app.end_after_count,
            Some(live),
            "the jump is waiting on this frame's count"
        );
        app.status_message = None;

        // And a count for some frame that is long gone fails.
        let _ = app.handle(&AppEvent::BackgroundLenFailed {
            len_generation: live.wrapping_sub(1),
        });

        assert_eq!(
            app.end_after_count,
            Some(live),
            "the End is still waiting on its own count, which has not failed"
        );
        assert_eq!(
            app.status_message, None,
            "and nothing is said about a count the user is not waiting on"
        );
    }

    /// An App on a remote dataset of a hundred rows that has not been counted yet, its
    /// buffer forty rows in.
    ///
    /// The receiver comes back with it so a test can read what the App sent.
    fn uncounted_remote_app() -> (crate::App, std::sync::mpsc::Receiver<crate::AppEvent>) {
        use crate::widgets::datatable::{DataTableState, RemoteFiles};
        use crate::{App, OpenOptions};
        use polars::prelude::*;
        use std::sync::Arc;

        let frame = || df!("id" => (0..100i64).collect::<Vec<_>>()).unwrap().lazy();
        let mut lf = frame();
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let mut state = DataTableState::from_schema_and_lazyframe(
            schema,
            frame(),
            &OpenOptions::default(),
            None,
        )
        .unwrap();
        state.set_remote_source();
        state.set_remote_files(RemoteFiles {
            urls: Arc::new(vec!["one".to_string()]),
            scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(frame())),
            count: Arc::new(|| Ok(vec![vec![100]])),
            offsets: None,
        });
        state.visible_rows = 10;
        // As far as the buffer reached, of a hundred. This is the number the bar prints
        // when nothing tells it the count failed, and printing it is the harm: a
        // confident partial where a "?" belongs.
        state.num_rows = 40;

        let (tx, rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.load_active = true;
        app.apply_schema_ready(state, None, &OpenOptions::default(), None);
        (app, rx)
    }

    /// The bottom line of a rendered App — the control bar, as a string.
    fn control_bar(app: &mut crate::App) -> String {
        use ratatui::buffer::Buffer;
        use ratatui::layout::Rect;
        use ratatui::widgets::Widget;

        let area = Rect::new(0, 0, 120, 24);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        (0..area.width)
            .map(|x| buf[(x, area.height - 1)].symbol().to_string())
            .collect()
    }

    /// The bar says `?` when this frame's count failed, rather than the partial the
    /// buffer happened to reach.
    ///
    /// The widget's own `?` has a test; what had none is the App deciding to ask for it.
    /// Two bugs were found in and around `len_count_failed` and the suite noticed
    /// neither, because nothing rendered the bar: deleting the write that produces `?`
    /// left the whole workspace green.
    #[test]
    fn the_bar_says_question_mark_when_the_count_failed() {
        use crate::AppEvent;

        let (mut app, _rx) = uncounted_remote_app();
        let live = app.data_table_state.as_ref().unwrap().len_generation();

        assert!(
            !control_bar(&mut app).contains("? rows"),
            "nothing has failed yet"
        );

        let _ = app.handle(&AppEvent::BackgroundLenFailed {
            len_generation: live,
        });

        let bar = control_bar(&mut app);
        assert!(
            bar.contains("? rows"),
            "the count failed, so the total is unknown: {bar:?}"
        );
    }

    /// A count that failed for a frame that is gone does not take the `?` off the frame
    /// that is here.
    ///
    /// `len_count_failed` is one slot and the bar reads it against the frame on screen.
    /// Written for whichever count failed last, an orphan — a join, a query, a filter or
    /// a sort takes a fresh `len_generation` without stopping the count already running
    /// — overwrote the live frame's own failure. `count_unknown` then went false and the
    /// bar printed the number the buffer had reached, plainly, on a dataset whose count
    /// failed.
    #[test]
    fn a_dead_frames_failed_count_leaves_this_frames_question_mark_alone() {
        use crate::AppEvent;

        let (mut app, _rx) = uncounted_remote_app();
        let live = app.data_table_state.as_ref().unwrap().len_generation();

        let _ = app.handle(&AppEvent::BackgroundLenFailed {
            len_generation: live,
        });
        assert!(
            control_bar(&mut app).contains("? rows"),
            "this frame's count failed"
        );

        // And now a count orphaned by an earlier frame change fails too.
        let _ = app.handle(&AppEvent::BackgroundLenFailed {
            len_generation: live.wrapping_sub(1),
        });

        let bar = control_bar(&mut app);
        assert!(
            bar.contains("? rows"),
            "a stranger's failure says nothing about this frame: {bar:?}"
        );
    }

    /// A look, set up as `ClassifyThenOpen` leaves it.
    #[cfg(test)]
    fn a_look_is_out(app: &mut crate::App, path: &std::path::Path) -> u64 {
        app.classify_requests = app.classify_requests.wrapping_add(1);
        let id = app.classify_requests;
        app.classify_inflight = Some(crate::ClassifyRequest {
            id,
            path: path.to_path_buf(),
            browsing: app.home.browsing.clone(),
        });
        app.busy = true;
        id
    }

    /// An answer nobody is waiting for is dropped — and the busy state it was holding
    /// goes with it, except where something else has taken that over.
    ///
    /// `spawn_bg` sets `busy` and this answer is the only thing that comes back, so a
    /// drop that does not clear it holds every key for the rest of the session. The
    /// exception is the one the other handlers rely on: a bumped `task_generation` means
    /// an `Open` or a collect set `busy` itself, and clearing it here takes the throbber
    /// off a load that is still running.
    #[test]
    fn a_classify_answer_nobody_is_waiting_for_leaves_the_right_busy_behind() {
        use crate::{App, AppEvent, InputMode};

        type MovedOn = fn(&mut App);
        let cases: Vec<(&str, MovedOn, bool)> = vec![
            (
                "they went back to the data",
                |app: &mut App| app.input_mode = InputMode::Normal,
                false,
            ),
            (
                "the browse moved under it",
                |app: &mut App| app.home.browsing = Some(std::path::PathBuf::from("/elsewhere")),
                false,
            ),
            (
                "an open took the generation",
                |app: &mut App| {
                    app.task_generation = app.task_generation.wrapping_add(1);
                    app.busy = true;
                },
                true,
            ),
        ];

        for (what, moved_on, busy_after) in cases {
            let (tx, _rx) = std::sync::mpsc::channel();
            let mut app = App::new(tx, crate::tests::test_runtime());
            app.enter_home();
            let path = std::path::PathBuf::from("/mnt/share/orders");
            let request = a_look_is_out(&mut app, &path);
            let generation = app.task_generation;

            moved_on(&mut app);
            let moved_to = app.home.browsing.clone();

            let follow = app.event(&AppEvent::BackgroundKindReady {
                generation,
                request,
                path: path.clone(),
                found: Some(crate::discover::EntryKind::MultiFile),
                jump: false,
            });

            assert!(follow.is_none(), "nothing was opened when {what}");
            assert_eq!(
                app.home.browsing, moved_to,
                "and it did not browse into the answer's path when {what}"
            );
            assert_eq!(
                app.is_busy(),
                busy_after,
                "busy after {what}: an answer puts down the busy it was holding, and \
                 only that one"
            );
            assert!(
                app.classify_inflight.is_none(),
                "and the look is no longer outstanding when {what}"
            );
        }
    }

    /// A newer look replaces an older one, and the older answer touches nothing.
    ///
    /// Every key acts on the home screen even while `busy`, so a second Enter is
    /// reachable. Refusing it meant a look at a share that never answers killed the
    /// feature for the rest of the session, silently — and the older answer must not put
    /// down the busy state the newer one is holding.
    #[test]
    fn a_newer_look_replaces_an_older_one() {
        use crate::{App, AppEvent};

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.enter_home();
        let first = std::path::PathBuf::from("/mnt/share/aaa");
        let stale = a_look_is_out(&mut app, &first);
        let generation = app.task_generation;

        // A second Enter, at a row the user moved to while the first was out.
        let second = std::path::PathBuf::from("/mnt/share/bbb");
        let _ = app.event(&AppEvent::ClassifyThenOpen {
            path: second.clone(),
            jump: false,
        });
        assert_eq!(
            app.classify_inflight.as_ref().map(|r| r.path.as_path()),
            Some(second.as_path()),
            "the newer look is the one being waited on"
        );

        // And the older answer arrives.
        let follow = app.event(&AppEvent::BackgroundKindReady {
            generation,
            request: stale,
            path: first,
            found: Some(crate::discover::EntryKind::MultiFile),
            jump: false,
        });

        assert!(follow.is_none(), "the stale answer opened nothing");
        assert!(
            app.classify_inflight.is_some(),
            "and did not cancel the look that replaced it"
        );
        assert!(app.is_busy(), "nor put down its busy state");
    }

    /// Going home puts down a look that may never answer.
    ///
    /// The case the whole path exists for is a share that has gone away, where the worker
    /// sits forever. Without this the keyboard waits with it: `abandon_load` clears
    /// `busy` only for a load, and a look is not one.
    #[test]
    fn going_home_does_not_wait_on_a_look_that_may_never_answer() {
        use crate::App;

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.enter_home();
        a_look_is_out(&mut app, std::path::Path::new("/mnt/gone/orders"));
        app.home.status = Some("Looking at orders...".to_string());

        app.enter_home();

        assert!(
            !app.is_busy(),
            "the keyboard is not waiting on a dead share"
        );
        assert!(app.classify_inflight.is_none(), "and the look is put down");
        assert_eq!(app.home.status, None, "with its line");
    }

    /// A typed path the worker could not find comes back to the prompt with the text in
    /// it, the way a local one never left.
    #[test]
    fn a_typed_path_that_is_not_there_comes_back_to_the_prompt() {
        use crate::{App, AppEvent};

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.enter_home();
        let path = std::path::PathBuf::from("/mnt/share/nope");
        let request = a_look_is_out(&mut app, &path);
        let generation = app.task_generation;

        let follow = app.event(&AppEvent::BackgroundKindReady {
            generation,
            request,
            path: path.clone(),
            found: None,
            jump: true,
        });

        assert!(follow.is_none());
        assert!(
            app.home
                .status
                .as_deref()
                .is_some_and(|s| s.contains("No such path")),
            "it says so: {:?}",
            app.home.status
        );
        assert!(app.home.path_input_active, "and the prompt is back");
        assert_eq!(
            app.home.path_input,
            path.display().to_string(),
            "with the path still in it"
        );
    }

    /// A parked End's message does not follow the user off the dataset.
    ///
    /// It parks without setting `busy`, so `abandon_load`'s cleanup — which is a load's
    /// — did not reach it, and Ctrl+O left it set. Painted on the home screen it replaces
    /// every key chip on the bar with a sentence about a dataset the user has left, and
    /// the failure that follows writes an error there that nothing ever clears.
    #[test]
    fn a_parked_end_does_not_put_its_message_on_the_home_screen() {
        use crate::AppEvent;
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

        let (mut app, _rx) = uncounted_remote_app();
        let waiting = app.data_table_state.as_ref().unwrap().len_generation();
        let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
        assert!(control_bar(&mut app).contains("Counting rows"), "parked");

        app.enter_home();

        let bar = control_bar(&mut app);
        assert!(
            !bar.contains("Counting rows"),
            "the home bar is the home screen's: {bar:?}"
        );
        assert!(
            bar.contains("Enter"),
            "and it still has its keys rather than a sentence: {bar:?}"
        );

        // And the count it was waiting on then fails, with the user somewhere else.
        // (Both halves of the fix are exercised: `abandon_load` clears the message on
        // the way out, and the gate below keeps it off a view that is not the table.)
        let _ = app.handle(&AppEvent::BackgroundLenFailed {
            len_generation: waiting,
        });
        let bar = control_bar(&mut app);
        assert!(
            !bar.contains("Could not count the rows"),
            "an error about a dataset they have left is not the home screen's news: \
             {bar:?}"
        );
    }

    /// And it does not reappear on the next dataset either.
    ///
    /// The message is deliberately *not* cleared on the way out: the End is still parked,
    /// and coming back to the same table with Esc should still say so. What must not
    /// happen is it greeting a different dataset — which it does not, because opening one
    /// puts up its own line. This pins that, since nothing else would notice if the order
    /// of those two ever changed.
    #[test]
    fn a_parked_end_does_not_put_its_message_on_the_next_dataset() {
        use crate::{OpenOptions, widgets::datatable::DataTableState};
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        use polars::prelude::*;
        use std::sync::Arc;

        let (mut app, _rx) = uncounted_remote_app();
        let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
        assert!(control_bar(&mut app).contains("Counting rows"), "parked");

        app.enter_home();

        // And they open something else, which installs its own frame.
        let rows = || df!("id" => &[1i64, 2, 3]).unwrap().lazy();
        let mut lf = rows();
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let next = DataTableState::from_schema_and_lazyframe(
            schema,
            rows(),
            &OpenOptions::default(),
            None,
        )
        .unwrap();
        app.load_active = true;
        app.apply_schema_ready(next, None, &OpenOptions::default(), None);
        app.busy = false;

        let bar = control_bar(&mut app);
        assert!(
            !bar.contains("Counting rows"),
            "the new dataset's bar is not the old one's: {bar:?}"
        );
    }

    /// The chart view's bar is the chart's, not a parked End's.
    ///
    /// `abandon_load` does not run here — the user has not left the dataset — so this is
    /// the gate on its own: the message is still set, and the view it belongs to is not
    /// the one on screen. Once the chart is ready `chart_preparing()` goes false, and
    /// before the gate the bar read "Counting rows to find the end..." where the chart keys
    /// belong.
    #[test]
    fn a_parked_end_does_not_put_its_message_on_the_chart_view() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

        let (mut app, _rx) = uncounted_remote_app();
        let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
        assert!(control_bar(&mut app).contains("Counting rows"), "parked");

        app.input_mode = crate::InputMode::Chart;
        app.chart_modal.active = true;

        let bar = control_bar(&mut app);
        assert!(
            app.status_message.is_some(),
            "the End is still waiting, and the field still says so"
        );
        assert!(
            !bar.contains("Counting rows"),
            "but the chart's bar is the chart's: {bar:?}"
        );
    }

    /// An End waiting on a count whose frame is gone, whose count then fails, is retired
    /// without saying anything.
    ///
    /// The frame it was counting has been replaced, so its failure says nothing about the
    /// one on screen and cannot answer the End that was waiting on it. Reached by nothing
    /// in the suite until now: deleting the branch left every test green.
    #[test]
    fn a_failed_count_for_a_frame_that_is_gone_retires_its_end_quietly() {
        use crate::AppEvent;
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

        let (mut app, _rx) = uncounted_remote_app();
        let waiting = app.data_table_state.as_ref().unwrap().len_generation();
        let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
        assert_eq!(app.end_after_count, Some(waiting));
        assert_eq!(
            app.status_message.as_deref(),
            Some(crate::App::COUNTING_FOR_END),
            "the status says the count is running"
        );

        // A question of the dataset takes a fresh generation out from under the count.
        let state = app.data_table_state.as_mut().unwrap();
        state.defer_collect = true;
        state.query("select doubled: id * 2".to_string());
        state.defer_collect = false;
        assert_ne!(
            app.data_table_state.as_ref().unwrap().len_generation(),
            waiting,
            "the frame the count belongs to is gone"
        );

        let _ = app.handle(&AppEvent::BackgroundLenFailed {
            len_generation: waiting,
        });

        assert_eq!(
            app.end_after_count, None,
            "the End it belonged to is retired"
        );
        let bar = control_bar(&mut app);
        assert!(
            !bar.contains("Counting rows"),
            "the status it put up comes down: {bar:?}"
        );
        assert!(
            !bar.contains("Could not count the rows"),
            "and does not become an error about a frame the user is no longer looking \
             at: {bar:?}"
        );
    }

    /// The bar says a count is running for an End, and says when it failed.
    ///
    /// Both messages were written and painted by nothing: the status line was shown only
    /// while `busy`, and an End waiting on a remote count parks without setting it —
    /// deliberately, so keys keep working. Three code paths existed to take a message
    /// down that could never appear.
    #[test]
    fn the_bar_says_it_is_counting_for_an_end_and_says_when_that_failed() {
        use crate::AppEvent;
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

        let (mut app, _rx) = uncounted_remote_app();
        let waiting = app.data_table_state.as_ref().unwrap().len_generation();

        let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
        assert!(
            !app.busy,
            "the jump parked rather than blocking the keyboard"
        );
        let bar = control_bar(&mut app);
        assert!(
            bar.contains("Counting rows"),
            "and the line says why the view has not moved: {bar:?}"
        );

        let _ = app.handle(&AppEvent::BackgroundLenFailed {
            len_generation: waiting,
        });
        let bar = control_bar(&mut app);
        assert!(
            bar.contains("Could not count the rows"),
            "and says so when the count it was waiting on fails: {bar:?}"
        );
    }

    /// An End pressed on the directory the user walked away from does not move the one
    /// they opened next.
    ///
    /// `end_after_count` names a `len_generation`, which says nothing about which
    /// dataset it belonged to — so it has to be put down when a dataset is, the way
    /// `end_when_the_footers_land` already is. Without that, a stale count returning
    /// after the user has opened something else hands the new dataset the old one's
    /// key: it starts a count it was never asked for and scrolls itself to the bottom
    /// when that count lands.
    #[test]
    fn an_end_pressed_on_the_dataset_they_left_does_not_move_the_next_one() {
        use crate::widgets::datatable::{DataTableState, RemoteFiles};
        use crate::{App, AppEvent, OpenOptions};
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        use polars::prelude::*;
        use std::sync::Arc;

        let remote_state = |n: i64| {
            let rows = move || df!("id" => (0..n).collect::<Vec<_>>()).unwrap().lazy();
            let mut lf = rows();
            let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
            let mut state = DataTableState::from_schema_and_lazyframe(
                schema,
                rows(),
                &OpenOptions::default(),
                None,
            )
            .unwrap();
            state.set_remote_source();
            state.set_remote_files(RemoteFiles {
                urls: Arc::new(vec!["one".to_string()]),
                scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
                count: Arc::new(move || Ok(vec![vec![n as usize]])),
                offsets: None,
            });
            state.visible_rows = 10;
            state
        };

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.load_active = true;
        app.apply_schema_ready(remote_state(100), None, &OpenOptions::default(), None);
        let theirs = app.data_table_state.as_ref().unwrap().len_generation();

        // End on the first directory, before its count lands.
        let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
        assert_eq!(
            app.end_after_count,
            Some(theirs),
            "the jump is waiting on that directory's count"
        );

        // And they open another one instead.
        app.load_active = true;
        app.apply_schema_ready(remote_state(500), None, &OpenOptions::default(), None);
        assert_eq!(
            app.end_after_count, None,
            "the key they pressed in the directory they left does not come with them"
        );

        // The first directory's count finally arrives.
        let mut follow = app.event(&AppEvent::BackgroundLenReady {
            len_generation: theirs,
            num_rows: 100,
            file_row_groups: None,
        });
        while let Some(event) = follow {
            follow = app.event(&event);
        }
        let next = app.data_table_state.as_ref().unwrap().len_generation();
        assert_ne!(
            app.end_after_count,
            Some(next),
            "and the directory on screen has not inherited it"
        );

        // Even once its own count lands, as it would.
        let mut follow = app.event(&AppEvent::BackgroundLenReady {
            len_generation: next,
            num_rows: 500,
            file_row_groups: None,
        });
        while let Some(event) = follow {
            follow = app.event(&event);
        }
        assert_eq!(
            app.data_table_state.as_ref().unwrap().start_row,
            0,
            "the directory they are looking at stays where they left it, at the top"
        );
    }

    /// Every background spawn takes a lease, and gets it back however it ends.
    ///
    /// This is the whole of #221's mechanism: the decision "is it safe to bump
    /// `task_generation`?" is a count, not a list of the kinds of work that might be
    /// running. A new kind of gated task is covered by going through `spawn_bg`, which
    /// is how every one of them is spawned.
    #[test]
    fn a_spawn_takes_a_lease_and_the_event_returns_it() {
        use crate::{App, AppEvent};

        let (tx, rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        assert!(!app.work_a_bump_would_strand(), "nothing is running yet");

        app.spawn_bg("Working...", |_task_gen, _tx| {});
        assert!(
            app.work_a_bump_would_strand(),
            "the spawn leased the generation"
        );

        // The worker finishes and its lease is dropped, which sends the event.
        let finished = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("the lease reports back");
        assert!(matches!(finished, AppEvent::BackgroundWorkFinished { .. }));
        let _ = app.handle(&finished);
        assert!(
            !app.work_a_bump_would_strand(),
            "and the generation is free again"
        );
    }

    /// Work a cancel passed holds nothing up. Polars cannot stop the query, so the worker
    /// runs on with its lease; the bump made its answer stale, and the table must not
    /// wait on it. A lease on the new generation still counts.
    #[test]
    fn a_cancelled_lease_does_not_hold_the_generation() {
        use crate::{App, AppEvent};

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        let old = app.lease_for_tests();
        let leased_on = app.task_generation;
        app.task_generation = app.task_generation.wrapping_add(1);
        assert!(
            !app.work_a_bump_would_strand(),
            "the abandoned worker is not waited on"
        );

        let current = app.lease_for_tests();
        assert!(app.work_a_bump_would_strand());

        // The abandoned worker finishing does not release the current one.
        std::mem::forget(old);
        let _ = app.handle(&AppEvent::BackgroundWorkFinished {
            generation: leased_on,
        });
        assert!(app.work_a_bump_would_strand());
        std::mem::forget(current);
        let _ = app.handle(&AppEvent::BackgroundWorkFinished {
            generation: app.task_generation,
        });
        assert!(!app.work_a_bump_would_strand());
    }

    /// A worker that panics still returns its lease.
    ///
    /// The hazard `schema_union::Pass` was built for: a count that never comes back down
    /// is a permanent "something is waiting", and here that would mean the buffer never
    /// collects again for the rest of the session.
    #[test]
    fn a_panicking_worker_still_returns_its_lease() {
        use crate::{App, AppEvent};

        let (tx, rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.spawn_bg("Working...", |_task_gen, _tx| panic!("worker died"));
        assert!(app.work_a_bump_would_strand());

        let finished = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("the lease reports back even from a panic");
        assert!(matches!(finished, AppEvent::BackgroundWorkFinished { .. }));
        let _ = app.handle(&finished);
        assert!(
            !app.work_a_bump_would_strand(),
            "the generation is free again rather than leased forever"
        );
    }

    /// Skipping the lease is a deliberate act rather than an oversight.
    ///
    /// Two spawns do. The buffer collect, whose answer is simply asked for again if a
    /// bump throws it away. And the look at a directory named on the command line, whose
    /// answer is *meant* to be thrown away when the user moves on — leased, it made a
    /// seventeen-second look hold the next dataset's buffer collect behind it, after
    /// Ctrl+O had been offered as the way out.
    ///
    /// `spawn_bg` leases by construction, so a new kind of gated background work is
    /// accounted for without anyone remembering to account for it. The two ways around
    /// it are `spawn_bg_replaceable` and `spawn_bg_inner`, and this counts both, over
    /// every file in the crate rather than this one — they are private to the crate
    /// root, which every module below it can reach.
    ///
    /// It cannot catch a raw `runtime.spawn_blocking` that captures `task_generation`
    /// itself. That is a different shape, and the three that exist do not carry a
    /// generation at all.
    #[test]
    fn an_unleased_spawn_is_a_deliberate_act() {
        // Split so this test's own needles are not among the things it finds.
        let needles = [
            (concat!("spawn_bg_", "replaceable("), 2usize),
            (concat!("spawn_bg_", "inner("), 2usize),
        ];
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut sources = Vec::new();
        let mut stack = vec![src];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir)
                .expect("the crate's own source")
                .flatten()
            {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.extension().is_some_and(|e| e == "rs") {
                    sources.push(std::fs::read_to_string(&path).expect("a source file"));
                }
            }
        }
        assert!(sources.len() > 20, "the crate's sources were found");

        for (needle, expected) in needles {
            let found: usize = sources.iter().map(|s| s.matches(needle).count()).sum();
            assert_eq!(
                found, expected,
                "`{needle}` appears {found} times, not {expected}. Two spawns skip the \
                 lease on purpose: the buffer collect, whose answer is asked for again \
                 if a bump throws it away, and the look at a directory named on the \
                 command line, whose answer is meant to be thrown away. Anything else \
                 that skips it can be stranded by a bump, silently. See GenerationLease."
            );
        }
    }

    /// A handler returning a continuation does not let the errands behind it in.
    ///
    /// `App::handle` runs the owed re-read and the owed collect at its tail, after
    /// `dispatch_event` has already returned the follow-up — so this is a window inside
    /// one event, before `EventPump` has seen the continuation and taken a lease for it.
    /// `Open` is the case that costs most: it bumps `task_generation`, sets
    /// `awaiting_dataset` and returns `DoLoadScanPaths` without spawning anything, so
    /// nothing holds a lease at all. An owed collect going in there bumps the generation
    /// the scan is about to be spawned against, and `BackgroundSchemaReady`'s mismatch
    /// branch returns without resetting anything: the file never opens.
    #[test]
    fn a_continuation_does_not_let_the_errands_behind_it_in() {
        use crate::widgets::datatable::DataTableState;
        use crate::{App, AppEvent, OpenOptions};
        use polars::prelude::*;
        use std::sync::Arc;

        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("next.csv");
        std::fs::write(&path, "name,age\nada,36\n").expect("write csv");

        let rows = || df!("id" => &[1i64, 2, 3]).unwrap().lazy();
        let mut lf = rows();
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let state = DataTableState::from_schema_and_lazyframe(
            schema,
            rows(),
            &OpenOptions::default(),
            None,
        )
        .unwrap();

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.load_active = true;
        app.apply_schema_ready(state, None, &OpenOptions::default(), None);

        // A collect owed to the dataset on screen, waiting for the generation to be free.
        app.collect_owed = Some((app.dataset_generation, "Loading buffer...".to_string()));
        assert!(
            !app.work_a_bump_would_strand(),
            "nothing holds the generation: the errand would go in on the next event"
        );

        // And the user opens something else.
        let out = app
            .handle(&AppEvent::Open(vec![path], OpenOptions::default()))
            .expect("the open is not a key");
        assert!(out.is_some(), "the open returned a continuation");
        assert!(
            app.collect_owed.is_some(),
            "the collect is still owed rather than run: running it here would bump the \
             generation the open has just taken for its scan"
        );
    }

    /// An open parked on the download confirmation holds the generation while it waits.
    ///
    /// The one errand that waits on neither a worker nor a continuation: nothing is
    /// running, the open is very much unfinished, and the wait is as long as the user
    /// takes to answer. A collect starting meanwhile bumps `task_generation`, and the
    /// download they are about to agree to then answers a generation nothing matches.
    #[cfg(any(feature = "http", feature = "cloud"))]
    #[test]
    fn a_download_waiting_on_the_user_holds_the_generation() {
        use crate::{App, AppEvent, OpenOptions, PendingDownload};
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.load_active = true;
        app.awaiting_dataset = true;
        assert!(!app.work_a_bump_would_strand(), "nothing is running yet");

        let pending = PendingDownload::Http {
            url: "https://example.invalid/data.parquet".to_string(),
            size: Some(1024),
            options: OpenOptions::default(),
        };
        let _ = app.handle(&AppEvent::BackgroundRemoteSizeReady {
            generation: app.task_generation(),
            pending: Box::new(pending),
        });

        assert!(app.confirmation_modal.active, "the user is being asked");
        assert!(
            app.work_a_bump_would_strand(),
            "and the generation is held for as long as they take to answer"
        );

        // Declining puts the errand down, and the generation with it.
        let _ = app.key(&KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        let _ = app.handle(&AppEvent::BackgroundWorkFinished {
            generation: app.task_generation(),
        });
        assert!(
            !app.work_a_bump_would_strand(),
            "nothing waits on it once the download is declined"
        );
    }

    /// A whole-table copy whose size is not known yet (the row count is still
    /// being read) must ask first, never collect an unknown amount unprompted.
    #[test]
    fn a_table_copy_with_no_size_yet_asks_first() {
        use crate::widgets::datatable::DataTableState;
        use crate::{App, OpenOptions};
        use polars::prelude::*;
        use std::sync::Arc;

        let rows = || df!("id" => &[1i64, 2, 3]).unwrap().lazy();
        let mut lf = rows();
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let state = DataTableState::from_schema_and_lazyframe(
            schema,
            rows(),
            &OpenOptions::default(),
            None,
        )
        .unwrap();
        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.load_active = true;
        app.apply_schema_ready(state, None, &OpenOptions::default(), None);

        let state = app.data_table_state.as_mut().unwrap();
        state.invalidate_num_rows();
        assert!(state.estimated_total_bytes().is_none());

        app.copy_modal.scope = crate::copy_modal::CopyScope::Table;
        let _ = app.perform_copy();
        assert!(
            app.confirmation_modal.active,
            "an unknown size asks; it never collects unprompted"
        );
        assert!(app.pending_copy.is_some());
    }

    /// A count landing while a load is in flight does not cancel the load.
    ///
    /// `BackgroundLenReady` answers an End by jumping to the end, which reaches
    /// `spawn_async_collect` with no key pressed and, on a large remote dataset, minutes
    /// after the one that was — long enough for the user to have opened something else.
    /// That bump threw the open's answer away, and `BackgroundSchemaReady`'s mismatch
    /// branch returns without resetting anything, so `awaiting_dataset`, `busy` and
    /// `loading_state` stayed set and the file never opened, silently, for the rest of
    /// the session.
    #[test]
    fn a_count_landing_during_a_load_does_not_bump_the_generation() {
        use crate::widgets::datatable::{DataTableState, RemoteFiles};
        use crate::{App, AppEvent, OpenOptions};
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
        use polars::prelude::*;
        use std::sync::Arc;

        let rows = || df!("id" => (0..100i64).collect::<Vec<_>>()).unwrap().lazy();
        let mut lf = rows();
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let mut state = DataTableState::from_schema_and_lazyframe(
            schema,
            rows(),
            &OpenOptions::default(),
            None,
        )
        .unwrap();
        state.set_remote_source();
        state.set_remote_files(RemoteFiles {
            urls: Arc::new(vec!["one".to_string()]),
            scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
            count: Arc::new(|| Ok(vec![vec![100]])),
            offsets: None,
        });
        state.visible_rows = 10;

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.load_active = true;
        app.apply_schema_ready(state, None, &OpenOptions::default(), None);

        // End on a dataset whose rows are not counted yet: the jump waits for the count.
        let waiting = app.data_table_state.as_ref().unwrap().len_generation();
        let _ = app.key(&KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
        assert_eq!(app.end_after_count, Some(waiting));

        // Meanwhile the user opens something else, which is waiting on this generation.
        let lease = app.lease_for_tests();
        let opening = app.task_generation();

        // And the count lands.
        let mut follow = app.event(&AppEvent::BackgroundLenReady {
            len_generation: waiting,
            num_rows: 100,
            file_row_groups: None,
        });
        while let Some(event) = follow {
            follow = app.event(&event);
        }

        assert_eq!(
            app.task_generation(),
            opening,
            "the open is still waiting on the generation the jump would have bumped"
        );
        assert!(
            app.collect_owed.is_some(),
            "and the jump's collect is owed rather than dropped"
        );

        // The open finishes, and the jump gets its turn.
        drop(lease);
        let _ = app.handle(&AppEvent::BackgroundWorkFinished {
            generation: app.task_generation(),
        });
        assert!(
            app.collect_owed.is_none(),
            "the collect the jump asked for runs once nothing is waiting"
        );
        assert_ne!(
            app.task_generation(),
            opening,
            "and it is what bumps the generation, now that it is safe to"
        );
    }

    /// A dataset waiting on an owed re-read still says its count is coming.
    ///
    /// The number a staged open holds is only as far as the buffer reached. While the
    /// pass is out the dataset says so itself, and the bar shows a spinner. A pass that
    /// fails takes that away — it gives up on the footers the moment it lands — and the
    /// count that would replace it is the one the errand is waiting to start. Between
    /// the two the bar has nothing marking the number provisional, and prints a prefix
    /// of six thousand files as `Rows: 70`, plainly, for as long as the work in front of
    /// the errand takes.
    #[test]
    fn a_dataset_owed_a_re_read_does_not_print_its_partial_as_the_total() {
        use crate::widgets::datatable::DataTableState;
        use crate::{App, AppEvent, LoadingState, OpenOptions};
        use polars::prelude::*;
        use ratatui::buffer::Buffer;
        use ratatui::layout::Rect;
        use ratatui::widgets::Widget;
        use std::sync::Arc;

        let rows = || df!("id" => (0..70i64).collect::<Vec<_>>()).unwrap().lazy();
        let mut lf = rows();
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let mut state = DataTableState::from_schema_and_lazyframe(
            schema,
            rows(),
            &OpenOptions::default(),
            None,
        )
        .unwrap();
        // As a staged open leaves it: a provisional from a short read, a pass still out.
        state.num_rows = 70;
        state.set_footers_pending(Arc::new(|_| None));

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.load_active = true;
        app.apply_schema_ready(state, None, &OpenOptions::default(), None);
        app.busy = false;

        let bar_says_seventy = |app: &mut App| {
            let area = Rect::new(0, 0, 100, 24);
            let mut buf = Buffer::empty(area);
            (&mut *app).render(area, &mut buf);
            (0..area.width)
                .map(|x| buf[(x, area.height - 1)].symbol().to_string())
                .collect::<String>()
                .contains("70 rows")
        };
        assert!(
            !bar_says_seventy(&mut app),
            "while the pass is out the dataset says its count is coming"
        );

        // An export is running and holds a lease, so the errand the failure raises has
        // to wait.
        app.loading_state = LoadingState::Exporting {
            file_path: std::path::PathBuf::from("/tmp/out.csv"),
            current_phase: "Collecting".to_string(),
            progress_percent: 0,
        };
        let _lease = app.lease_for_tests();
        let live = app.dataset_generation;
        App::record_footers(&app.pending_footers_result, live, None);
        let _ = app.handle(&AppEvent::BackgroundFootersJoined { generation: live });
        assert!(
            app.reread_owed.is_some(),
            "the fixture is a dataset owed a re-read it cannot have yet"
        );

        app.busy = false;
        assert!(
            !bar_says_seventy(&mut app),
            "and it goes on saying so while the count it is owed waits its turn"
        );
    }

    /// A query over a dataset still reading its footers still gets counted.
    ///
    /// The pass is bringing the *dataset's* count, which is not the count of a query's
    /// result — nobody else is going to take that one. Declining it because a pass is
    /// out leaves the row count spinning for as long as the query is open, and `End`
    /// saying it is counting rows while nothing is counting anything. It costs nothing
    /// to take: a frame that is not the scan does not read footers for its count.
    #[test]
    fn a_query_over_a_dataset_still_reading_its_footers_is_counted() {
        use crate::widgets::datatable::{DataTableState, RemoteFiles};
        use crate::{App, OpenOptions};
        use polars::prelude::*;
        use std::sync::Arc;

        let rows = || df!("id" => (0..100i64).collect::<Vec<_>>()).unwrap().lazy();
        let mut lf = rows();
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());

        let mut state = DataTableState::from_schema_and_lazyframe(
            schema,
            rows(),
            &OpenOptions::default(),
            None,
        )
        .unwrap();
        state.set_remote_source();
        state.set_remote_files(RemoteFiles {
            urls: Arc::new(vec!["one".to_string()]),
            scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
            count: Arc::new(|| Ok(vec![vec![100]])),
            offsets: None,
        });
        state.visible_rows = 10;
        // Still reading, so as the scan it rightly declines to count itself.
        state.set_footers_pending(Arc::new(|_| None));
        assert!(
            state.counts_itself_later(),
            "the pass is bringing this dataset's count"
        );

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.load_active = true;
        app.apply_schema_ready(state, None, &OpenOptions::default(), None);

        // And then the user asks a question of it, whose answer has a count of its own.
        let state = app.data_table_state.as_mut().unwrap();
        state.defer_collect = true;
        state.query("select doubled: id * 2".to_string());
        state.defer_collect = false;
        assert!(
            !state.counts_itself_later(),
            "which is not the count the pass is bringing, and nothing else will take it"
        );

        app.spawn_async_collect(App::LOADING_BUFFER);
        assert!(
            app.len_count_inflight.is_some(),
            "so it is taken, rather than the row count spinning while the query is open"
        );
    }

    /// Columns found for the dataset before this one join nothing to this one.
    ///
    /// Abandoning a load cancels nothing: the footers of a prefix the user has moved on
    /// from keep being read, and land afterwards. Without a generation that counts
    /// datasets put on screen they would be joined to whatever is there now — a directory
    /// gaining a column from a different directory entirely.
    #[test]
    fn a_pass_from_the_dataset_before_this_one_joins_nothing_to_it() {
        use crate::widgets::datatable::DataTableState;
        use crate::{App, OpenOptions};
        use polars::prelude::*;
        use std::sync::Arc;

        let dataset_of = |lf: LazyFrame| {
            let mut lf = lf;
            let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
            let footer = crate::schema_union::FileSchema {
                schema,
                rows: 1,
                file_bytes: 0,
                row_group_bytes: Vec::new(),
            };
            crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
        };
        let state_of = |lf: LazyFrame| {
            DataTableState::from_schema_and_lazyframe(
                dataset_of(lf.clone()).schema.clone(),
                lf,
                &OpenOptions::default(),
                None,
            )
            .unwrap()
        };

        let first = || df!("id" => &[1i64]).unwrap().lazy();
        let its_columns = || df!("id" => &[1i64], "oops" => &["a"]).unwrap().lazy();
        let second = || df!("other" => &[2i64]).unwrap().lazy();

        let (tx, rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());

        // The first dataset, staged.
        let mut state = state_of(first());
        state.set_footers_pending(Arc::new(move |_progress| {
            Some(crate::widgets::datatable::FootersFound {
                dataset: dataset_of(its_columns()),
                lf: its_columns(),
                file_rows: Vec::new(),
                files: Vec::new(),
                row_groups: Vec::new(),
                remote: None,
            })
        }));
        app.load_active = true;
        app.apply_schema_ready(state, None, &OpenOptions::default(), None);
        let reported = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("the first dataset's pass reports back");

        // The user opens something else before those columns arrive. Nothing here sets
        // the generation by hand: if opening a dataset does not move it, this test is
        // the one that notices.
        app.load_active = true;
        app.apply_schema_ready(state_of(second()), None, &OpenOptions::default(), None);

        let _ = app.handle(&reported);
        assert_eq!(
            app.data_table_state.as_ref().unwrap().get_column_order(),
            ["other"],
            "the directory on screen does not gain a column from the directory before it"
        );
        assert!(
            app.footers_held.is_none(),
            "and they are not kept waiting for a dataset that is gone"
        );
    }

    /// A footer pass that could not read them waits for work already asked for, too.
    ///
    /// The failure branch re-reads for a different reason than the success branch — the
    /// pass brought no count, so the dataset has to go and count itself the ordinary way
    /// — but it goes through the same collect, and that collect bumps `task_generation`
    /// just the same. It used to run on the spot, the one way into the collect that
    /// asked nothing about what was already running: an export in its collect phase
    /// never wrote its file and said nothing about it.
    ///
    /// Revert `reread_owed` and this fails on the first assert: the generation moves
    /// while the export is still waiting on it.
    #[test]
    fn a_pass_that_failed_waits_for_work_already_asked_for() {
        use crate::widgets::datatable::DataTableState;
        use crate::{App, AppEvent, LoadingState, OpenOptions};
        use polars::prelude::*;
        use std::sync::Arc;

        let frame = || df!("id" => &[1i64]).unwrap().lazy();
        let dataset_of = |lf: LazyFrame| {
            let mut lf = lf;
            let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
            let footer = crate::schema_union::FileSchema {
                schema,
                rows: 1,
                file_bytes: 0,
                row_group_bytes: Vec::new(),
            };
            crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
        };

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        let state = DataTableState::from_schema_and_lazyframe(
            dataset_of(frame()).schema.clone(),
            frame(),
            &OpenOptions::default(),
            None,
        )
        .unwrap();
        app.load_active = true;
        app.apply_schema_ready(state, None, &OpenOptions::default(), None);

        // An export is collecting: it holds a lease on this exact generation, and its
        // answer is thrown away if anything bumps it.
        app.loading_state = LoadingState::Exporting {
            file_path: std::path::PathBuf::from("/tmp/out.csv"),
            current_phase: "Collecting".to_string(),
            progress_percent: 0,
        };
        let lease = app.lease_for_tests();
        let waiting_on = app.task_generation();

        // The pass comes back empty-handed for the dataset on screen.
        let live = app.dataset_generation;
        App::record_footers(&app.pending_footers_result, live, None);
        let _ = app.handle(&AppEvent::BackgroundFootersJoined { generation: live });

        assert_eq!(
            app.task_generation(),
            waiting_on,
            "the export is still waiting on the answer this app would have thrown away"
        );
        assert_eq!(
            app.reread_owed,
            Some(live),
            "and the re-read the dataset is owed is remembered, not dropped"
        );

        // The export finishes, and the errand gets its turn on the next event.
        app.loading_state = LoadingState::Idle;
        drop(lease);
        let _ = app.handle(&AppEvent::BackgroundWorkFinished {
            generation: app.task_generation(),
        });
        let _ = app.handle(&AppEvent::Update);

        assert!(
            app.task_generation() != waiting_on,
            "the dataset gets the collect it was owed once nothing is waiting on the \
             generation — without it, it never counts itself at all"
        );
        assert!(
            app.reread_owed.is_none(),
            "and the errand is done rather than run again on every event"
        );
    }

    /// Columns arriving during work already asked for wait for it, rather than
    /// cancelling it.
    ///
    /// The re-read after a join goes through the ordinary collect, which bumps
    /// `task_generation` — the token the export is waiting on. Bumped underneath one,
    /// the export's own answer is thrown away when it arrives: in its collect phase
    /// that means the file is never written and nothing is said about it. The pass runs
    /// for minutes on the prefixes this is for, so an export started at the open is
    /// certain to be inside that window.
    #[test]
    fn columns_arriving_during_work_already_asked_for_wait_for_it() {
        use crate::widgets::datatable::{DataTableState, FootersFound};
        use crate::{App, AppEvent, GenerationLease, OpenOptions};
        use polars::prelude::*;
        use std::sync::Arc;

        let frame = || df!("id" => &[1i64]).unwrap().lazy();
        let wider = || df!("id" => &[1i64], "oops" => &["a"]).unwrap().lazy();
        let dataset_of = |lf: LazyFrame| {
            let mut lf = lf;
            let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
            let footer = crate::schema_union::FileSchema {
                schema,
                rows: 1,
                file_bytes: 0,
                row_group_bytes: Vec::new(),
            };
            crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
        };

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        let state = DataTableState::from_schema_and_lazyframe(
            dataset_of(frame()).schema.clone(),
            frame(),
            &OpenOptions::default(),
            None,
        )
        .unwrap();
        app.load_active = true;
        app.apply_schema_ready(state, None, &OpenOptions::default(), None);

        // Work whose answer the join would throw away. A lease stands for all of it —
        // an open, an export, an analysis — which is the point: the decision is no
        // longer a list of the kinds that happen to exist today. The chart is here
        // beside it because it is the one that a bump would *not* strand: it is
        // prepared against the frame, and the join takes a fresh one of those too.
        type Start = fn(&mut App) -> Option<GenerationLease>;
        let under_way: Vec<(&str, Start)> = vec![
            ("leased background work", |app: &mut App| {
                Some(app.lease_for_tests())
            }),
            ("a chart", |app: &mut App| {
                app.chart_inflight = Some(crate::ChartInflight {
                    dataset: None,
                    request: crate::ChartRequest::XRange {
                        x_column: "id".to_string(),
                        row_limit: None,
                    },
                    stale: false,
                });
                None
            }),
        ];
        let put_away = |app: &mut App, lease: Option<GenerationLease>| {
            // The lease is released by dropping it, which sends the event the count is
            // decremented by — behind whatever result the work had already sent.
            drop(lease);
            let _ = app.handle(&AppEvent::BackgroundWorkFinished {
                generation: app.task_generation(),
            });
            app.chart_inflight = None;
        };

        for (what, start) in under_way {
            let lease = start(&mut app);
            let waiting_on = app.task_generation();
            app.footers_held = Some((
                app.dataset_generation,
                FootersFound {
                    dataset: dataset_of(wider()),
                    lf: wider(),
                    file_rows: Vec::new(),
                    files: Vec::new(),
                    row_groups: Vec::new(),
                    remote: None,
                },
            ));
            let _ = app.handle(&AppEvent::Update);

            assert_eq!(
                app.task_generation(),
                waiting_on,
                "{what} is still waiting on the answer this app would have thrown away"
            );
            assert!(
                app.footers_held.is_some(),
                "and the columns wait their turn behind {what}"
            );
            put_away(&mut app, lease);
        }

        // Nothing under way now, and they go in.
        let _ = app.handle(&AppEvent::Update);
        assert_eq!(
            app.data_table_state
                .as_ref()
                .unwrap()
                .get_column_order()
                .last()
                .map(String::as_str),
            Some("oops"),
            "once nothing is waiting on an answer, the columns join"
        );
    }

    /// Columns held while the user was in a query are not joined to the next dataset.
    ///
    /// Held columns outlive the dataset they belong to: the user is inside a query when
    /// they arrive, so they wait — and the user may then open something else entirely
    /// rather than clear the query. Opening clears the slot but not what is already
    /// held, and the first keypress on the new directory is where the held columns would
    /// go in: one directory's schema, scan and file list installed into another.
    #[test]
    fn columns_held_for_one_dataset_are_not_given_to_the_next() {
        use crate::widgets::datatable::{DataTableState, FootersFound};
        use crate::{App, AppEvent, OpenOptions};
        use polars::prelude::*;
        use std::sync::Arc;

        let first = || df!("id" => &[1i64]).unwrap().lazy();
        let its_columns = || df!("id" => &[1i64], "oops" => &["a"]).unwrap().lazy();
        let second = || df!("other" => &[2i64]).unwrap().lazy();
        let dataset_of = |lf: LazyFrame| {
            let mut lf = lf;
            let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
            let footer = crate::schema_union::FileSchema {
                schema,
                rows: 1,
                file_bytes: 0,
                row_group_bytes: Vec::new(),
            };
            crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
        };
        let state_of = |lf: LazyFrame| {
            DataTableState::from_schema_and_lazyframe(
                dataset_of(lf.clone()).schema.clone(),
                lf,
                &OpenOptions::default(),
                None,
            )
            .unwrap()
        };

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.load_active = true;
        app.apply_schema_ready(state_of(first()), None, &OpenOptions::default(), None);

        // The user is in a query when this dataset's columns arrive, so they wait.
        app.data_table_state
            .as_mut()
            .unwrap()
            .query("select doubled: id * 2".to_string());
        app.footers_held = Some((
            app.dataset_generation,
            FootersFound {
                dataset: dataset_of(its_columns()),
                lf: its_columns(),
                file_rows: Vec::new(),
                files: Vec::new(),
                row_groups: Vec::new(),
                remote: None,
            },
        ));
        let _ = app.handle(&AppEvent::Update);
        assert!(app.footers_held.is_some(), "waiting, as they should be");

        // And instead of clearing the query, the user opens something else.
        app.load_active = true;
        app.apply_schema_ready(state_of(second()), None, &OpenOptions::default(), None);
        let _ = app.handle(&AppEvent::Update);

        assert_eq!(
            app.data_table_state.as_ref().unwrap().get_column_order(),
            ["other"],
            "the directory now on screen is not given the last one's columns"
        );
        assert!(
            app.footers_held.is_none(),
            "and they are let go rather than waiting on for a third dataset"
        );
    }

    /// The event that wakes the app does not decide whose answer is in the slot.
    ///
    /// Two passes run at once when a second large prefix is opened, and the newer one
    /// can overwrite the slot before the older one's event is handled. Deciding by the
    /// event would drain the newer answer and throw it away on the older event's
    /// generation — and the newer event, arriving next, would find the slot empty. The
    /// dataset on screen would wait for columns that had already been and gone, with
    /// nothing to say so: the pass is over, so even the count in the bar is silent.
    #[test]
    fn a_late_event_from_an_old_pass_does_not_throw_away_the_live_answer() {
        use crate::widgets::datatable::{DataTableState, FootersFound};
        use crate::{App, AppEvent, OpenOptions};
        use polars::prelude::*;
        use std::sync::Arc;

        let frame = || df!("id" => &[1i64]).unwrap().lazy();
        let wider = || df!("id" => &[1i64], "oops" => &["a"]).unwrap().lazy();
        let dataset_of = |lf: LazyFrame| {
            let mut lf = lf;
            let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
            let footer = crate::schema_union::FileSchema {
                schema,
                rows: 1,
                file_bytes: 0,
                row_group_bytes: Vec::new(),
            };
            crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
        };

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        let state = DataTableState::from_schema_and_lazyframe(
            dataset_of(frame()).schema.clone(),
            frame(),
            &OpenOptions::default(),
            None,
        )
        .unwrap();
        app.load_active = true;
        app.apply_schema_ready(state, None, &OpenOptions::default(), None);

        // This dataset's own pass has finished and put its answer in the slot.
        let live = app.dataset_generation;
        App::record_footers(
            &app.pending_footers_result,
            live,
            Some(FootersFound {
                dataset: dataset_of(wider()),
                lf: wider(),
                file_rows: Vec::new(),
                files: Vec::new(),
                row_groups: Vec::new(),
                remote: None,
            }),
        );

        // And the event that reaches the loop first belongs to the prefix the user
        // opened before this one, whose pass was slower.
        let _ = app.handle(&AppEvent::BackgroundFootersJoined {
            generation: live.wrapping_sub(1),
        });

        assert_eq!(
            app.data_table_state
                .as_ref()
                .unwrap()
                .get_column_order()
                .last()
                .map(String::as_str),
            Some("oops"),
            "the answer in the slot is this dataset's, and it is the one that is used"
        );
    }

    /// A slower pass from an older dataset does not displace a newer one's answer.
    ///
    /// Opening a second large prefix does not stop the first one reading, so two passes
    /// can be in flight and finish in either order. If the older one wrote last, the
    /// generation in the slot would disagree with the generation on the event and both
    /// would be thrown away — leaving the dataset on screen permanently short of the
    /// columns its own pass had already found.
    #[test]
    fn an_older_pass_finishing_late_does_not_displace_a_newer_one() {
        use crate::App;
        use polars::prelude::*;
        use std::sync::{Arc, Mutex};

        let found = |name: &str| {
            let mut lf = df!(name => &[1i64]).unwrap().lazy();
            let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
            let footer = crate::schema_union::FileSchema {
                schema,
                rows: 1,
                file_bytes: 0,
                row_group_bytes: Vec::new(),
            };
            crate::widgets::datatable::FootersFound {
                dataset: crate::schema_union::union_sampled(1, &[0], &[Some(footer)]),
                lf,
                file_rows: Vec::new(),
                files: Vec::new(),
                row_groups: Vec::new(),
                remote: None,
            }
        };
        let name_in =
            |slot: &Mutex<Option<(u64, Option<crate::widgets::datatable::FootersFound>)>>| {
                slot.lock().unwrap().as_ref().map(|(g, f)| {
                    let f = f.as_ref().expect("recorded with something in it");
                    (
                        *g,
                        f.dataset.schema.iter_names().next().unwrap().to_string(),
                    )
                })
            };

        let slot = Mutex::new(None);
        assert!(
            App::record_footers(&slot, 7, Some(found("newer"))),
            "the newer pass answers first"
        );
        assert!(
            !App::record_footers(&slot, 6, Some(found("older"))),
            "and the older one, finishing after it, is turned away"
        );
        assert_eq!(
            name_in(&slot),
            Some((7, "newer".to_string())),
            "so what is waiting is still the newer dataset's"
        );

        // The ordinary case is unaffected: a pass for the dataset now on screen goes in
        // over whatever an abandoned one left behind.
        assert!(
            App::record_footers(&slot, 8, Some(found("newest"))),
            "a later dataset's pass takes the slot"
        );
        assert_eq!(name_in(&slot), Some((8, "newest".to_string())));
    }

    /// Columns that arrive while the user is inside a query wait for them to leave it.
    ///
    /// The scan a query is built on is not the frame on screen: rebuilding it wider
    /// underneath takes away the columns the query named, and the table goes to a
    /// Polars "unable to find column" where a moment ago there were rows. So the
    /// columns are held, and get in when the view comes back to the data.
    #[test]
    fn columns_arriving_under_a_query_wait_rather_than_break_it() {
        use crate::widgets::datatable::DataTableState;
        use crate::{App, AppEvent, OpenOptions};
        use polars::prelude::*;
        use std::sync::Arc;

        let frame = || df!("id" => &[1i64, 2], "v" => &[10i64, 20]).unwrap().lazy();
        let wider = || {
            df!("id" => &[1i64, 2], "v" => &[10i64, 20], "oops" => &["a", "b"])
                .unwrap()
                .lazy()
        };
        let dataset_of = |lf: LazyFrame| {
            let mut lf = lf;
            let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
            let footer = crate::schema_union::FileSchema {
                schema,
                rows: 2,
                file_bytes: 0,
                row_group_bytes: Vec::new(),
            };
            crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
        };

        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        let state = DataTableState::from_schema_and_lazyframe(
            dataset_of(frame()).schema.clone(),
            frame(),
            &OpenOptions::default(),
            None,
        )
        .unwrap();
        app.load_active = true;
        app.apply_schema_ready(state, None, &OpenOptions::default(), None);

        // The user asks a question of the two columns that are there.
        let table = app.data_table_state.as_mut().unwrap();
        table.query("select doubled: v * 2".to_string());
        assert!(table.error.is_none(), "the query runs: {:?}", table.error);
        let asked = table.get_column_order().to_vec();

        // And the rest of the footers land underneath it.
        let generation = app.dataset_generation;
        app.footers_held = Some((
            generation,
            crate::widgets::datatable::FootersFound {
                dataset: dataset_of(wider()),
                lf: wider(),
                file_rows: Vec::new(),
                files: Vec::new(),
                row_groups: Vec::new(),
                remote: None,
            },
        ));
        let _ = app.handle(&AppEvent::Update);

        let table = app.data_table_state.as_ref().unwrap();
        assert_eq!(
            table.get_column_order(),
            asked.as_slice(),
            "the query's own columns are still what is on screen"
        );
        assert!(
            table.error.is_none(),
            "and it has not been broken out from under: {:?}",
            table.error
        );
        assert!(
            app.footers_held.is_some(),
            "the columns are kept, not thrown away"
        );

        // The user clears the query — an empty one is how that is said — and now they
        // can get in.
        app.data_table_state.as_mut().unwrap().query(String::new());
        let _ = app.handle(&AppEvent::Update);
        assert_eq!(
            app.data_table_state
                .as_ref()
                .unwrap()
                .get_column_order()
                .last()
                .map(String::as_str),
            Some("oops"),
            "the columns join once the view is back on the data"
        );
        assert!(app.footers_held.is_none(), "with nothing left waiting");
    }

    /// A dataset that opened from two footers gets the rest, through the app.
    ///
    /// The cloud test covers the pass itself; this covers everything between it and the
    /// screen — that the app starts it without marking itself busy, that what it finds
    /// reaches the dataset on screen, and that a pass belonging to a dataset the user
    /// has since left cannot join its columns to the one that replaced it.
    #[test]
    fn a_staged_open_joins_what_its_footers_found() {
        use crate::widgets::datatable::DataTableState;
        use crate::{App, AppEvent, OpenOptions};
        use polars::prelude::*;
        use std::sync::Arc;

        let frame = || df!("id" => &[1i64, 2], "v" => &[10i64, 20]).unwrap().lazy();
        let wider = || {
            df!("id" => &[1i64, 2], "v" => &[10i64, 20], "oops" => &["a", "b"])
                .unwrap()
                .lazy()
        };
        let dataset_of = |lf: LazyFrame| {
            let mut lf = lf;
            let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
            let footer = crate::schema_union::FileSchema {
                schema,
                rows: 2,
                file_bytes: 0,
                row_group_bytes: Vec::new(),
            };
            crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
        };

        // The wider frame counts every time it is read, so the join can be asked
        // whether it read the data — which, on the thread drawing the screen and
        // against a dataset in a bucket, is the one thing it must not do.
        let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = {
            let reads = reads.clone();
            move || {
                let reads = reads.clone();
                wider().with_column(col("id").map(
                    move |s| {
                        reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        Ok(s)
                    },
                    |_schema: &Schema, field: &Field| Ok(field.clone()),
                ))
            }
        };

        let (tx, rx) = std::sync::mpsc::channel();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let mut app = App::new(tx, runtime.handle().clone());
        let mut state = DataTableState::from_schema_and_lazyframe(
            dataset_of(frame()).schema.clone(),
            frame(),
            &OpenOptions::default(),
            None,
        )
        .unwrap();
        // What the pass behind the open will find: one column more.
        state.set_footers_pending(Arc::new(move |_progress| {
            Some(crate::widgets::datatable::FootersFound {
                dataset: dataset_of(counted()),
                lf: counted(),
                file_rows: Vec::new(),
                files: Vec::new(),
                row_groups: Vec::new(),
                remote: None,
            })
        }));
        app.load_active = true;
        // Installed the way an open installs it, rather than dropped into the field:
        // handing the pass over is one line of `apply_schema_ready`, and a test that
        // starts the pass itself would not notice that line going missing.
        app.apply_schema_ready(state, None, &OpenOptions::default(), None);
        assert!(
            !app.is_busy(),
            "the dataset is on screen and must keep working while the rest are read"
        );
        let joined = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("the pass reports back");
        let AppEvent::BackgroundFootersJoined { generation } = joined else {
            panic!("expected the footers to be reported, got another event");
        };

        // It lands after a glance at the home screen, which leaves the dataset up and
        // clears `load_active`. One keystroke there and back must not strand the
        // dataset on two footers for the rest of the session.
        app.abandon_load();
        let _ = app.handle(&AppEvent::BackgroundFootersJoined { generation });
        // Read again, not asked to be read again. The join drops the buffer, so a
        // request that goes on to be ignored — as a step of the open's chain is, once
        // the load is over — leaves the table with nothing to show at the moment it was
        // to show more.
        assert!(
            app.collect_inflight.is_some(),
            "the rows on screen were read through the narrow frame and are read again"
        );
        assert_eq!(
            app.data_table_state
                .as_ref()
                .unwrap()
                .get_column_order()
                .last()
                .map(String::as_str),
            Some("oops"),
            "the column the pass found joins the dataset on screen"
        );
        assert_eq!(
            reads.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "and nothing was read to do it: this runs on the thread drawing the frame, \
             and against a bucket the read it would do is a `len()` over every file"
        );
    }

    /// Only one query type is returned; SQL overrides fuzzy over DSL. Used when saving templates.
    #[test]
    fn test_active_query_settings_only_one_set() {
        use super::active_query_settings;

        let (q, sql, fuzzy) = active_query_settings("", "", "");
        assert!(q.is_none() && sql.is_none() && fuzzy.is_none());

        let (q, sql, fuzzy) = active_query_settings("select a", "SELECT 1", "foo");
        assert!(q.is_none() && sql.as_deref() == Some("SELECT 1") && fuzzy.is_none());

        let (q, sql, fuzzy) = active_query_settings("select a", "", "foo bar");
        assert!(q.is_none() && sql.is_none() && fuzzy.as_deref() == Some("foo bar"));

        let (q, sql, fuzzy) = active_query_settings("  select a  ", "", "");
        assert!(q.as_deref() == Some("select a") && sql.is_none() && fuzzy.is_none());
    }
}

/// Which CSV string columns to trim and parse (date/datetime/time/duration/int/float). Default: all. None = disabled (e.g. --no-parse-strings).
#[derive(Clone, Debug)]
pub enum ParseStringsTarget {
    /// Apply to all string columns.
    All,
    /// Apply only to these columns (must exist and be string type).
    Columns(Vec<String>),
}

#[derive(Clone)]
pub struct OpenOptions {
    pub delimiter: Option<u8>,
    pub has_header: Option<bool>,
    pub skip_lines: Option<usize>,
    pub skip_rows: Option<usize>,
    /// Skip this many rows at the end of the file (e.g. vendor footer or trailing garbage). Applied after load for CSV.
    pub skip_tail_rows: Option<usize>,
    pub compression: Option<CompressionFormat>,
    /// When set, bypass extension-based format detection and use this format (e.g. for URLs or temp files without extension).
    pub format: Option<FileFormat>,
    pub pages_lookahead: Option<usize>,
    pub pages_lookback: Option<usize>,
    pub max_buffered_rows: Option<usize>,
    pub max_buffered_mb: Option<usize>,
    pub row_numbers: bool,
    pub row_start_index: usize,
    /// When true, use hive load path for directory/glob; single file uses normal load.
    pub hive: bool,
    /// Data files in the directory being opened that this read passes over, by format and
    /// count.
    ///
    /// A directory of more than one format is read as the commonest of them — a thousand
    /// CSVs and one stray JSON is a directory of CSVs — and this is what the stray was,
    /// so the dataset can say what it left out rather than the directory being refused
    /// over it. Empty for every other open, which is all of them but one.
    pub left_out: Vec<(FileFormat, usize)>,
    /// Set when the directory being opened is a lake table and this read is of its plain
    /// files: `"Delta"`, `"Iceberg"` or `"Hudi"`.
    ///
    /// The files are not the table. A delete leaves its rows on disk, an update leaves
    /// the version it replaced, and compaction leaves both sides — so this read counts
    /// rows no query of the table would return. datui does it anyway, because the
    /// alternative was a directory the user could see and could not read at all, and
    /// every other engine at least lets you look. What makes it honest rather than wrong
    /// is that it is never silent: a note and a chip in the control bar say so, and both
    /// are load-bearing.
    pub read_as_plain_files_of: Option<&'static str>,
    /// How the directory's own files differed, when they did.
    ///
    /// Only for the formats with no footer. A Parquet dataset's footers are read
    /// anyway, and say this per column and per file in far more detail — which columns,
    /// in how many files, and where — so saying it twice would be one vague note above
    /// several exact ones.
    pub files_disagree: crate::schema_union::Disagreement,
    /// When true (default), infer Hive/partitioned Parquet schema from one file for faster "Caching schema". When false, use Polars collect_schema().
    pub single_spine_schema: bool,
    /// `--template NAME`: the template to apply to the dataset named on the command
    /// line, once it is on screen. Applied to that open only; what later opens get
    /// is `[templates] auto_apply`'s business.
    pub template: Option<String>,
    /// When true, CSV and JSON string columns that look like dates or ISO 8601 timestamps become Date or Datetime.
    pub parse_dates: bool,
    /// When set, trim and parse CSV string columns: None = off, Some(true) = all columns, Some(cols) = those columns only.
    pub parse_strings: Option<ParseStringsTarget>,
    /// Sample size (rows) for inferring types when parse_strings is enabled; single file or multiple/partitioned.
    pub parse_strings_sample_rows: usize,
    /// When true, decompress compressed CSV into memory (eager read). When false (default), decompress to a temp file and use lazy scan.
    pub decompress_in_memory: bool,
    /// Directory for decompression temp files. None = system default (e.g. TMPDIR).
    pub temp_dir: Option<std::path::PathBuf>,
    /// Excel sheet: 0-based index or sheet name (CLI only).
    pub excel_sheet: Option<String>,
    /// S3/compatible settings from the command line. They outrank the environment and
    /// the config file; see `effective_cloud`.
    pub s3_endpoint_url_override: Option<String>,
    pub s3_access_key_id_override: Option<String>,
    pub s3_secret_access_key_override: Option<String>,
    pub s3_region_override: Option<String>,
    /// `--cloud-discover`, outranking `[cloud] discover`.
    pub cloud_discover_override: Option<crate::config::CloudDiscover>,
    /// When true, use Polars streaming engine for LazyFrame collect when the streaming feature is enabled.
    pub polars_streaming: bool,
    /// No effect since Polars 0.55: the eager pivot that crashed on a Date/Datetime index is
    /// gone. Kept so `--workaround-pivot-date-index` and the Python option still parse.
    pub workaround_pivot_date_index: bool,
    /// Null value specs for CSV: global strings and/or "COL=VAL" for per-column. Empty = use Polars default.
    pub null_values: Option<Vec<String>>,
    /// Number of rows to use when inferring CSV schema. None = Polars default (100). Larger values reduce risk of inferring wrong type (e.g. int then N/A).
    pub infer_schema_length: Option<usize>,
    /// When true, CSV reader ignores parse errors and continues with the next batch.
    pub ignore_errors: bool,
    /// When true, show the debug overlay (session info, performance, query, etc.).
    pub debug: bool,
}

impl OpenOptions {
    pub fn new() -> Self {
        Self {
            delimiter: None,
            has_header: None,
            skip_lines: None,
            skip_rows: None,
            skip_tail_rows: None,
            left_out: Vec::new(),
            read_as_plain_files_of: None,
            files_disagree: Default::default(),
            compression: None,
            format: None,
            pages_lookahead: None,
            pages_lookback: None,
            max_buffered_rows: None,
            max_buffered_mb: None,
            row_numbers: false,
            row_start_index: 1,
            hive: false,
            single_spine_schema: true,
            template: None,
            parse_dates: true,
            parse_strings: None,
            parse_strings_sample_rows: 1000,
            decompress_in_memory: false,
            temp_dir: None,
            excel_sheet: None,
            s3_endpoint_url_override: None,
            s3_access_key_id_override: None,
            s3_secret_access_key_override: None,
            s3_region_override: None,
            cloud_discover_override: None,
            polars_streaming: true,
            workaround_pivot_date_index: true,
            null_values: None,
            infer_schema_length: None,
            ignore_errors: false,
            debug: false,
        }
    }
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenOptions {
    pub fn with_skip_lines(mut self, skip_lines: usize) -> Self {
        self.skip_lines = Some(skip_lines);
        self
    }

    pub fn with_skip_rows(mut self, skip_rows: usize) -> Self {
        self.skip_rows = Some(skip_rows);
        self
    }

    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = Some(delimiter);
        self
    }

    pub fn with_has_header(mut self, has_header: bool) -> Self {
        self.has_header = Some(has_header);
        self
    }

    /// The separator a delimited file is read with: `--delimiter` when given, else
    /// the one its format implies (`FileFormat::separator`).
    pub fn separator_or(&self, format_default: u8) -> u8 {
        self.delimiter.unwrap_or(format_default)
    }

    pub fn with_compression(mut self, compression: CompressionFormat) -> Self {
        self.compression = Some(compression);
        self
    }

    pub fn with_workaround_pivot_date_index(mut self, workaround_pivot_date_index: bool) -> Self {
        self.workaround_pivot_date_index = workaround_pivot_date_index;
        self
    }

    /// When loading CSV: use Polars try_parse_dates only if parse_strings is not set.
    /// When parse_strings is set we do our own date parsing (with strict: false), so we disable
    /// Polars' try_parse_dates to avoid "could not find an appropriate format" errors.
    pub fn csv_try_parse_dates(&self) -> bool {
        self.parse_strings.is_none() && self.parse_dates
    }

    /// The S3 settings every cloud path uses: the command line over the environment
    /// over the `[cloud]` config. `run()` folds this into the config the `App` keeps,
    /// so opening, sizing, downloading, discovery and listing all see one answer and a
    /// bucket that is listed is reached the way it will be opened. The environment is
    /// read here, not when the options are built, so a caller that starts from
    /// `OpenOptions::default()` — the Python bindings do — still honours it.
    pub fn effective_cloud(
        &self,
        cloud: &crate::config::CloudConfig,
    ) -> crate::config::CloudConfig {
        let mut merged = cloud.clone();
        merged.merge(crate::config::CloudConfig::from_env(&crate::cloud_env::var));
        merged.merge(crate::config::CloudConfig {
            s3_endpoint_url: self.s3_endpoint_url_override.clone(),
            s3_access_key_id: self.s3_access_key_id_override.clone(),
            s3_secret_access_key: self.s3_secret_access_key_override.clone(),
            s3_region: self.s3_region_override.clone(),
            discover: self.cloud_discover_override.clone(),
            ..Default::default()
        });
        merged
    }
}

impl OpenOptions {
    /// Create OpenOptions from CLI args and config, with CLI args taking precedence
    pub fn from_args_and_config(args: &cli::Args, config: &AppConfig) -> Self {
        let mut opts = OpenOptions::new();

        // A file's layout: command line only. Set in config, these applied to every
        // file opened and silently cut rows from the ones they did not describe (#289).
        opts.delimiter = args.delimiter;
        opts.skip_lines = args.skip_lines;
        opts.skip_rows = args.skip_rows;
        opts.skip_tail_rows = args.skip_tail_rows;
        opts.has_header = args.no_header.map(|no_header| !no_header);
        opts.template = args.template.clone();

        // Compression: CLI only (auto-detect from extension when not specified)
        opts.compression = args.compression;

        // Format: CLI only (auto-detect from extension when not specified)
        opts.format = args.format;

        // Display options: CLI args override config
        opts.pages_lookahead = args
            .pages_lookahead
            .or(Some(config.display.pages_lookahead));
        opts.pages_lookback = args.pages_lookback.or(Some(config.display.pages_lookback));
        opts.max_buffered_rows = Some(config.display.max_buffered_rows);
        opts.max_buffered_mb = Some(config.display.max_buffered_mb);

        // Row numbers: CLI flag overrides config
        opts.row_numbers = args.row_numbers || config.display.row_numbers;

        // Row start index: CLI arg overrides config
        opts.row_start_index = args
            .row_start_index
            .unwrap_or(config.display.row_start_index);

        // Hive partitioning: CLI only (no config option yet)
        opts.hive = args.hive;

        // Single-spine schema: CLI overrides config; default true
        opts.single_spine_schema = args
            .single_spine_schema
            .or(config.file_loading.single_spine_schema)
            .unwrap_or(true);

        // CSV date inference: CLI overrides config; default true
        opts.parse_dates = args
            .parse_dates
            .or(config.file_loading.parse_dates)
            .unwrap_or(true);

        // Parse strings (trim + type inference). Default: all CSV string columns. --no-parse-strings disables; --parse-strings=COL limits to columns.
        if args.no_parse_strings {
            opts.parse_strings = None;
        } else if !args.parse_strings.is_empty() {
            let has_all = args.parse_strings.iter().any(|s| s.is_empty());
            opts.parse_strings = Some(if has_all {
                ParseStringsTarget::All
            } else {
                let cols: Vec<String> = args
                    .parse_strings
                    .iter()
                    .filter(|s| !s.is_empty())
                    .cloned()
                    .collect::<std::collections::HashSet<_>>()
                    .into_iter()
                    .collect();
                ParseStringsTarget::Columns(cols)
            });
        } else if config.file_loading.parse_strings == Some(false) {
            opts.parse_strings = None;
        } else {
            opts.parse_strings = Some(ParseStringsTarget::All);
        }
        opts.parse_strings_sample_rows = config
            .file_loading
            .parse_strings_sample_rows
            .unwrap_or(1000);

        // Decompress-in-memory: CLI overrides config; default false (decompress to temp, use scan)
        opts.decompress_in_memory = args
            .decompress_in_memory
            .or(config.file_loading.decompress_in_memory)
            .unwrap_or(false);

        // Temp directory for decompression: CLI overrides config; default None (system temp)
        opts.temp_dir = args.temp_dir.clone().or_else(|| {
            config
                .file_loading
                .temp_dir
                .as_ref()
                .map(std::path::PathBuf::from)
        });

        // Excel sheet (CLI only)
        opts.excel_sheet = args.excel_sheet.clone();

        // S3/compatible flags. The environment is folded in by `effective_cloud`.
        opts.s3_endpoint_url_override = args.s3_endpoint_url.clone();
        opts.s3_access_key_id_override = args.s3_access_key_id.clone();
        opts.s3_secret_access_key_override = args.s3_secret_access_key.clone();
        opts.s3_region_override = args.s3_region.clone();
        // Already checked by clap, which takes the same words.
        opts.cloud_discover_override = args
            .cloud_discover
            .as_deref()
            .and_then(|text| text.parse().ok());

        opts.polars_streaming = config.performance.polars_streaming;

        opts.workaround_pivot_date_index = args.workaround_pivot_date_index.unwrap_or(true);

        // Debug: CLI flag overrides config
        opts.debug = args.debug || config.debug.enabled;

        // Null values: merge config list with CLI list (CLI appended); if either is non-empty, set
        let config_nulls = config.file_loading.null_values.as_deref().unwrap_or(&[]);
        let cli_nulls = &args.null_value;
        if config_nulls.is_empty() && cli_nulls.is_empty() {
            opts.null_values = None;
        } else {
            opts.null_values = Some(
                config_nulls
                    .iter()
                    .chain(cli_nulls.iter())
                    .cloned()
                    .collect(),
            );
        }

        // CSV schema inference: CLI overrides config; default 1000 (Polars default is 100)
        opts.infer_schema_length = args
            .infer_schema_length
            .or(config.file_loading.infer_schema_length)
            .or(Some(1000));

        // CSV ignore parse errors: CLI overrides config; default false
        opts.ignore_errors = args
            .ignore_errors
            .or(config.file_loading.ignore_errors)
            .unwrap_or(false);

        opts
    }
}

impl From<&cli::Args> for OpenOptions {
    fn from(args: &cli::Args) -> Self {
        // Use default config if creating from args alone
        let config = AppConfig::default();
        Self::from_args_and_config(args, &config)
    }
}

pub enum AppEvent {
    Key(KeyEvent),
    Open(Vec<PathBuf>, OpenOptions),
    /// Open with an existing LazyFrame (e.g. from Python binding); no file load.
    OpenLazyFrame(Box<LazyFrame>, OpenOptions),
    /// Scan paths and build LazyFrame; then emit DoLoadSchema (phased loading).
    DoLoadScanPaths(Vec<PathBuf>, OpenOptions),
    /// Build LazyFrame for CSV with --parse-strings (phase already set to "Scanning string columns" so UI shows it).
    DoLoadCsvWithParseStrings(Vec<PathBuf>, OpenOptions),
    /// Perform HTTP download (next loop so "Downloading" can render first). Then emit DoLoadFromHttpTemp.
    #[cfg(feature = "http")]
    DoDownloadHttp(String, OpenOptions),
    /// Perform S3 download to temp (next loop so "Downloading" can render first). Then emit DoLoadFromHttpTemp.
    #[cfg(feature = "cloud")]
    DoDownloadS3ToTemp(String, OpenOptions),
    /// Perform GCS download to temp (next loop so "Downloading" can render first). Then emit DoLoadFromHttpTemp.
    #[cfg(feature = "cloud")]
    DoDownloadGcsToTemp(String, OpenOptions),
    /// HTTP, S3, or GCS download finished; temp path is ready. Scan it and continue load.
    #[cfg(any(feature = "http", feature = "cloud"))]
    DoLoadFromHttpTemp(PathBuf, OpenOptions),
    /// A home listing built off-thread is ready.
    HomeListingReady {
        generation: u64,
        listing: Box<crate::home::Listing>,
    },
    /// A completed path, worked out off-thread.
    HomePathCompleted {
        generation: u64,
        /// What was typed when completion was asked for; a later keystroke makes the
        /// answer stale.
        typed: String,
        completed: String,
        candidates: usize,
    },
    /// A schema read off-thread for the highlighted dataset.
    HomeSchemaReady {
        generation: u64,
        path: PathBuf,
        preview: Option<crate::discover::SchemaPreview>,
    },
    /// Measurements for rows the home screen asked about, sent as each row is read so
    /// a slow row does not hold back the ones before it. `done` marks the end of the
    /// batch and frees the slot for the next one.
    ///
    /// No generation, unlike its neighbors: what a look found is keyed by path and
    /// true of that path whichever listing asked, so an answer that outlives its
    /// listing is still the answer.
    HomeMeasured {
        measured: Vec<(PathBuf, crate::home::Measured)>,
        done: bool,
    },
    /// What the rows on screen turned out to be. The same payload as
    /// [`AppEvent::HomeMeasured`] and folded in the same way: a kind is one of the
    /// things a look into a row produces.
    HomeClassified {
        measured: Vec<(PathBuf, crate::home::Measured)>,
        done: bool,
    },
    /// A batch of datasets found by the background search below the working
    /// directory. Sent repeatedly while the walk runs, so a cold tree fills in
    /// rather than arriving all at once at the end.
    HomeSearchBatch {
        generation: u64,
        root: PathBuf,
        found: Vec<crate::discover::Entry>,
        scanned: usize,
    },
    /// The background search has stopped, with `limited` saying why if it stopped
    /// short of walking everything.
    HomeSearchDone {
        generation: u64,
        root: PathBuf,
        scanned: usize,
        limited: Option<String>,
    },
    /// The cloud sources on this machine, with whatever was listed on an earlier run.
    /// Sent before anything is fetched, so the rows are there on the first frame.
    #[cfg(feature = "cloud")]
    HomeCloudSources {
        sources: Vec<crate::home::CloudSource>,
    },
    /// One source's buckets have been listed, or could not be. Each source reports on
    /// its own, so a slow endpoint holds up nobody else's row.
    #[cfg(feature = "cloud")]
    HomeCloudListed {
        id: String,
        buckets: Vec<PathBuf>,
        /// Lines for the details pane of each listed place that has any.
        details: Vec<(PathBuf, Vec<(String, String)>)>,
        /// `(short, detail)` when the listing failed.
        failure: Option<(String, String)>,
        listed_at: std::time::SystemTime,
    },
    /// A network root has been listed off-thread, or could not be.
    HomeProbeReady {
        root: PathBuf,
        rows: Option<Vec<crate::discover::Entry>>,
        /// The listing stopped at [`crate::discover::MAX_ENTRIES_PER_DIR`].
        cut_short: bool,
    },
    /// The rows of a network directory read so far, while its listing goes on.
    HomeProbeProgress {
        root: PathBuf,
        rows: Vec<crate::discover::Entry>,
    },
    /// What peeking inside some directories of a cloud listing found: the ones that are
    /// partitioned or Parquet datasets.
    HomeCloudKinds {
        kinds: Vec<(
            PathBuf,
            (crate::discover::EntryKind, crate::discover::Holds),
        )>,
        /// Directories whose peek failed or was lost: not answered, so not labelled as
        /// if they were.
        failed: Vec<PathBuf>,
    },
    /// A cloud listing was refused, with the service's reason.
    HomeProbeFailed {
        root: PathBuf,
        message: String,
    },
    /// Background scan finished; the LazyFrame is waiting in `pending_lazyframe_result`.
    BackgroundLazyFrameReady {
        generation: u64,
        path: Option<PathBuf>,
        options: OpenOptions,
    },
    /// Update phase to "Caching schema" and emit DoLoadSchemaBlocking so UI can draw before blocking.
    DoLoadSchema(Box<LazyFrame>, Option<PathBuf>, OpenOptions),
    /// Actually run collect_schema() and create state; then emit DoLoadBuffer (phased loading).
    DoLoadSchemaBlocking(Box<LazyFrame>, Option<PathBuf>, OpenOptions),
    /// First collect() on state; then emit Collect (phased loading).
    DoLoadBuffer,
    DoDecompress(Vec<PathBuf>, OpenOptions), // Internal event to perform decompression after UI shows "Decompressing"
    DoExport(PathBuf, ExportFormat, ExportOptions), // Internal event to perform export after UI shows progress
    DoExportCollect(PathBuf, ExportFormat, ExportOptions), // Collect data for export; then emit DoExportWrite
    DoExportWrite(PathBuf, ExportFormat, ExportOptions),   // Write collected DataFrame to file
    DoLoadParquetMetadata, // Load Parquet metadata when info panel is opened (deferred from render)
    Exit,
    Crash(String),
    Search(String),
    SqlSearch(String),
    FuzzySearch(String),
    Filter(Vec<FilterStatement>),
    Sort(Vec<String>, Vec<bool>), // Columns, and per column whether it runs descending
    ColumnOrder(Vec<String>, usize), // Column order, locked columns count
    Pivot(PivotSpec),
    Melt(MeltSpec),
    Export(PathBuf, ExportFormat, ExportOptions), // Path, format, options
    /// Collect and format the whole view off-thread for a table-scope copy.
    CopyTable {
        format: crate::clipboard::CopyFormat,
        header: bool,
    },
    /// A table-scope copy, collected and formatted; the write happens on the
    /// event thread, which owns the clipboard handle.
    BackgroundCopyReady {
        generation: u64,
        payload: crate::clipboard::Payload,
        rows: usize,
        format: crate::clipboard::CopyFormat,
    },
    ChartExport(PathBuf, ChartExportFormat, String, u32, u32), // path, format, title, width, height
    DoChartExport(PathBuf, ChartExportFormat, String, u32, u32), // Deferred: run chart export
    Collect,
    Update,
    Reset,
    Resize(u16, u16), // resized (width, height)
    DoScrollDown,     // Deferred scroll: perform page_down after one frame (throbber)
    DoScrollUp,       // Deferred scroll: perform page_up
    DoScrollNext,     // Deferred scroll: perform select_next (one row down)
    DoScrollPrev,     // Deferred scroll: perform select_previous (one row up)
    DoScrollEnd,      // Deferred scroll: jump to last page (throbber)
    DoScrollHome,     // Deferred scroll: jump to first page (throbber)
    DoScrollHalfDown, // Deferred scroll: half page down
    DoScrollHalfUp,   // Deferred scroll: half page up
    GoToLine(usize),  // Deferred: jump to line number (when collect needed)
    /// Run the next chunk of analysis (describe/distribution); drives per-column progress.
    AnalysisChunk,
    /// Run distribution analysis (deferred so progress overlay can show first).
    AnalysisDistributionCompute,
    /// Run correlation matrix (deferred so progress overlay can show first).
    AnalysisCorrelationCompute,
    /// Run the configured data-quality plan off the UI thread.
    AnalysisDataQualityCompute,
    /// Background task completed: the shared sample's rows, to show as a table.
    BackgroundSampleReady {
        generation: u64,
        df: polars::prelude::DataFrame,
        label: String,
    },
    /// Background task completed: describe/statistics results.
    BackgroundDescribeReady {
        generation: u64,
        results: crate::statistics::AnalysisResults,
    },
    /// Background task completed: distribution analysis results.
    BackgroundDistributionReady {
        generation: u64,
        results: crate::statistics::AnalysisResults,
    },
    /// Background task completed: correlation matrix results.
    BackgroundCorrelationReady {
        generation: u64,
        results: crate::statistics::AnalysisResults,
    },
    /// Background task completed: data-quality profile.
    BackgroundDataQualityReady {
        generation: u64,
        results: crate::data_quality::DataQualityResults,
        /// The rows a sampled run read, for the next run and drill to cut.
        kept: Option<KeptQualitySample>,
    },
    /// Background task completed: buffer data collected.
    /// The actual DataFrame is stored in App::pending_collect_result (to avoid cloning).
    BackgroundCollectReady {
        generation: u64,
    },
    /// A buffer collect failed. Reported as a `BackgroundError` when something waited
    /// on it; a load-ahead's failure is left for the page that needs those rows.
    BackgroundCollectFailed {
        generation: u64,
        message: String,
    },
    /// Background task completed: exact row count for the current LazyFrame. Applied to
    /// `data_table_state` only if `len_generation` still matches (the data is unchanged).
    /// Runs concurrently with — and independently of — the first buffer paint, so the
    /// count fills in the scrollbar/total without ever blocking the initial render.
    BackgroundLenReady {
        len_generation: u64,
        num_rows: usize,
        /// For a remote dataset of many files, the rows in each row group of each file,
        /// from their footers.
        file_row_groups: Option<Vec<Vec<usize>>>,
    },
    /// Background row count failed. Clears the in-flight marker so the count can be retried
    /// on a later interaction; the total stays provisional in the meantime.
    BackgroundLenFailed {
        len_generation: u64,
    },
    /// Every footer of a dataset that opened from two of them has now been read. What
    /// they say is in `App::pending_footers_result`; the columns they add join the
    /// dataset already on screen.
    BackgroundFootersJoined {
        generation: u64,
    },
    /// Background task completed: schema loaded and DataTableState constructed.
    /// The actual state is stored in App::pending_schema_result (to avoid cloning DataTableState).
    BackgroundSchemaReady {
        generation: u64,
        path: Option<PathBuf>,
        options: OpenOptions,
        debug_label: Option<String>,
    },
    /// Background task completed: chart data for one selection is prepared. The data is
    /// in `App::pending_chart_result`; it belongs to `App::chart_inflight`, which says
    /// whether it is still wanted.
    BackgroundChartReady,
    /// Background task completed: chart written to disk.
    BackgroundChartExportWritten {
        generation: u64,
        path: PathBuf,
        format: ChartExportFormat,
        result: Result<(), String>,
    },
    /// Background task completed: export data collected.
    BackgroundExportCollected {
        generation: u64,
        df: DataFrame,
        path: PathBuf,
        format: ExportFormat,
        options: ExportOptions,
    },
    /// Background task completed: file written to disk.
    BackgroundExportWritten {
        generation: u64,
        path: PathBuf,
        result: Result<(), String>,
    },
    /// Background task completed: the remote file's size is known, so the download can
    /// be put to the user. The probe is a network round trip and the HTTP one waits up
    /// to fifteen seconds, so it cannot be done on the event thread.
    #[cfg(any(feature = "http", feature = "cloud"))]
    BackgroundRemoteSizeReady {
        generation: u64,
        pending: Box<PendingDownload>,
    },
    /// Background task completed: remote file downloaded to temp path.
    #[cfg(any(feature = "http", feature = "cloud"))]
    BackgroundDownloadReady {
        generation: u64,
        temp_path: PathBuf,
        options: OpenOptions,
    },
    /// Background task failed.
    BackgroundError {
        generation: u64,
        message: String,
    },
    /// A [`GenerationLease`] was released: the work holding it has finished, however it
    /// finished. Sent by the lease's `Drop`, so it arrives behind whatever result the
    /// work sent first.
    BackgroundWorkFinished {
        generation: u64,
    },
    /// A directory named on the command line: look at it on a worker, then do with it
    /// whatever `Enter` on its row would do.
    ///
    /// The look reads footers, or the front of a spread of files, which for a directory
    /// of large Parquet is seconds. It is an event rather than a call so the first frame
    /// is drawn before it starts, and the wait has the directory's name on it, a spinner
    /// and a way out.
    LookThenOpenDirectory(PathBuf, OpenOptions),
    /// What the look found, back from the worker.
    DirectoryLookedAt {
        generation: u64,
        path: PathBuf,
        kind: discover::EntryKind,
        /// What a cloud directory's listing found, which picks its reader. `None` for a
        /// local directory, and for a cloud one whose listing was refused.
        holds: Option<Box<discover::Holds>>,
        options: Box<OpenOptions>,
    },
    /// Look at a path off the interface thread, then do with it whatever it turns out to
    /// need — browse into it, say it is a lake table, or open it.
    ///
    /// `exists`, `is_dir` and `classify_directory` are all filesystem calls, and the home
    /// screen is full of paths on mounts that may not answer. Doing them where the keys
    /// are read is an uninterruptible freeze with Ctrl+C on the same thread.
    ClassifyThenOpen {
        path: PathBuf,
        /// A jump — a path typed at `~` — rather than a row that was already listed. Esc
        /// then comes back from there to the listing, not up through wherever the path
        /// happens to sit.
        jump: bool,
    },
    /// The row of a grouped view that Enter drills into, read off the UI thread because
    /// the buffer did not hold it. The error is a message for the user.
    DrillRowRead {
        generation: u64,
        group_index: usize,
        row: std::result::Result<DataFrame, String>,
    },
    /// What [`AppEvent::ClassifyThenOpen`]'s worker found. `None` is a path that is not
    /// there.
    BackgroundKindReady {
        generation: u64,
        /// Which look this answers. A newer one replaces it, and the older answer is then
        /// not the one the user is waiting for — nor the owner of the busy state.
        request: u64,
        path: PathBuf,
        found: Option<discover::EntryKind>,
        jump: bool,
    },
}

/// A look at a path that is out on a worker, and what would make its answer stale.
///
/// `browsing` rather than `home_generation`: the question is whether the user is still
/// where they asked from, and the listing is rebuilt for reasons that are nothing to do
/// with them — a probe of some other root answering is enough. Gating on that made Enter
/// on a share row do nothing, at random.
struct ClassifyRequest {
    id: u64,
    path: PathBuf,
    /// Where the home screen was pointed when the look was asked for.
    browsing: Option<PathBuf>,
}

/// A lease on the current `task_generation`, held by background work whose answer
/// arrives once.
///
/// `task_generation` is the token `BackgroundSchemaReady`, `BackgroundExportCollected`,
/// `BackgroundExportWritten`, the three analysis results, `BackgroundLazyFrameReady`,
/// `BackgroundRemoteSizeReady`, `BackgroundDownloadReady` and `BackgroundError` are all
/// gated on. Bumping it while one is in flight throws that answer away when it arrives,
/// silently, and nothing asks again: an export that never writes its file, an analysis
/// left on its spinner, a dataset that never opens. So the bump waits for the lease.
///
/// Counted rather than enumerated. The predicate this replaced listed the kinds of work
/// that might be running, and was found short by one entry in three consecutive review
/// rounds; a lease is taken by [`App::spawn_bg`] itself, so the next kind of background
/// work is covered without anyone remembering to add it.
///
/// Released by `Drop`, which sends an event rather than touching the count directly: the
/// count lives on `App`, the lease lives on a worker thread, and the event is queued
/// behind the result that worker just sent — so the handler that consumes the result has
/// already run by the time the lease is retired. A worker that panics unwinds through
/// the same `Drop`, so a permanent "something is waiting" cannot be stranded that way.
/// This is [`crate::schema_union::Pass`]'s trick, for a count on the other side of a
/// channel.
///
/// One worker, though, not one errand. An errand of several phases hands off through the
/// event queue and holds no lease for an event at a time, so two other things take one:
/// [`crate::event_pump::EventPump`] while it holds a continuation it has not dispatched,
/// and `pending_download` while the confirmation modal waits on the user. Between them
/// the count covers a whole errand, which is what lets the predicate be only the count —
/// with one exception. `reread_after_the_footers_joined` sends its jump straight to the
/// channel, unleased, and that is safe only because both its callers have already checked
/// that nothing is waiting on the generation. A fourth handoff added that way would not
/// be.
///
/// A worker that never returns at all — a `hard` NFS mount, a wedged object-store read —
/// never drops its lease; that thread already leaves `busy` set for the session, so the
/// app is wedged with or without this, but the count does not rescue it.
struct GenerationLease {
    events: Sender<AppEvent>,
    /// The generation it was taken on. Work on an older one was cancelled by the bump
    /// that passed it, and its answer will be dropped, so it holds nothing up.
    generation: u64,
}

impl Drop for GenerationLease {
    fn drop(&mut self) {
        // Nobody to tell means the app is gone, and so is the count.
        let _ = self.events.send(AppEvent::BackgroundWorkFinished {
            generation: self.generation,
        });
    }
}

/// What [`App::handle`] did with an event: `Ok` carries the follow-up event to send,
/// if any; `Err` returns a key that arrived while the app was busy. Nothing was done
/// with that key and it was not dropped: the caller keeps it and offers it again once
/// the app is idle.
pub type EventOutcome = Result<Option<AppEvent>, KeyEvent>;

/// What <kbd>Enter</kbd> will do on the highlighted row.
///
/// Written so the control bar and the details pane can say it before it happens.
/// Every directory has two doors and the labels no longer decide access, which is only
/// worth anything if the screen says which key is which — a bar reading `Enter Open` on
/// a row where `Enter` goes inside teaches the wrong thing on the first try, and the
/// first try is the one that forms the impression.
///
/// A prediction, so it can drift from [`App::home_open_selected`], which is the thing
/// that actually decides. `test_the_bar_says_what_enter_will_really_do` pumps `Enter`
/// on one row of every shape and asserts the two agreed; that test is the reason this
/// is safe to read from the renderer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WhatEnter {
    /// Load the file on the row.
    OpensFile,
    /// Read the whole directory as one table: a hive root, a directory whose files are
    /// one table, or the `(all files)` row.
    OpensDirectory,
    /// Step into the directory. What `→` does too, on these rows.
    GoesInside,
    /// Look at the row first, then do whichever of the above the answer calls for.
    LooksFirst,
    /// Fold or unfold a section.
    FoldsSection,
    /// Show the rest of `RECENT`.
    ShowsMore,
    /// Show the files datui cannot open, as `Ctrl+A` does.
    ShowsHidden,
    /// Nothing to open and nowhere to go: an HTTP place, which has no listing to
    /// browse and says so.
    Explains,
    /// A file datui has no reader for. Enter says so, and the bar offers nothing.
    Nothing,
}

impl App {
    /// See [`WhatEnter`].
    pub fn what_enter_does(&self) -> WhatEnter {
        // One walk of the list, not four. Every `selected_*` helper rebuilds it, and this
        // runs from the control bar on every frame, beside a
        // `selected_directory_to_enter` that walks it once more.
        let rows = self.home.visible();
        let entry = match rows.get(self.home.selected) {
            // A place row browses into the place, which is what `→` does on it too, so
            // it is labelled the same and offered once. An HTTP place has no listing to
            // browse and says so instead.
            Some(home::Row::Place { path, .. }) => {
                return if home::place_is_browsable(path) {
                    WhatEnter::GoesInside
                } else {
                    WhatEnter::Explains
                };
            }
            Some(home::Row::Header { .. }) => return WhatEnter::FoldsSection,
            Some(home::Row::More { .. }) => return WhatEnter::ShowsMore,
            Some(home::Row::Hidden { .. }) => return WhatEnter::ShowsHidden,
            None => return WhatEnter::Explains,
            // The door reads the directory it names whatever that directory is labelled —
            // the lake tables included, which is the one row that reads them at all.
            Some(home::Row::Door { .. }) => return WhatEnter::OpensDirectory,
            Some(home::Row::Entry { entry, .. }) => *entry,
        };
        match entry.kind {
            discover::EntryKind::Unknown => WhatEnter::LooksFirst,
            discover::EntryKind::File => WhatEnter::OpensFile,
            discover::EntryKind::Other => WhatEnter::Nothing,
            discover::EntryKind::Hive | discover::EntryKind::MultiFile => WhatEnter::OpensDirectory,
            // A plain directory, and a lake table, whose files are not its rows.
            discover::EntryKind::Directory
            | discover::EntryKind::Delta
            | discover::EntryKind::Iceberg
            | discover::EntryKind::Hudi => WhatEnter::GoesInside,
        }
    }
}

/// What a read of a directory found out about itself on the way through.
///
/// Filled by the pass that actually picks the files and the reader, and carried back on
/// the options so the dataset can say it in the Notes. Everything here is about what
/// datui *did*, not about what the data is — the footer notes are the other half, and
/// they are written later, by whatever read the footers.
#[derive(Debug, Clone, Default)]
pub struct ReadReport {
    /// Data files in the directory this read passed over, by format and count. A
    /// directory of more than one format is read as the commonest of them; this is the
    /// rest.
    pub left_out: Vec<(FileFormat, usize)>,
    /// How the files read differed. See [`OpenOptions::files_disagree`].
    pub files_disagree: crate::schema_union::Disagreement,
    /// The reader the files were read with, where the read chose it: a directory's
    /// commonest format, or a file's extension. Carried back as `OpenOptions::format`,
    /// so what is on screen knows whether it has a header row to turn off.
    pub format: Option<FileFormat>,
}

/// Input for the shared run loop: open from file paths or from an existing LazyFrame (e.g. Python binding).
#[derive(Clone)]
pub enum RunInput {
    Paths(Vec<PathBuf>, OpenOptions),
    LazyFrame(Box<LazyFrame>, OpenOptions),
}

#[derive(Debug, Clone)]
pub struct ExportOptions {
    pub csv_delimiter: u8,
    pub csv_include_header: bool,
    /// Add a column naming the file each row came from, so a cell that is absent
    /// rather than null can still be told apart once the data has left datui.
    pub source_file: bool,
    pub csv_compression: Option<CompressionFormat>,
    pub json_compression: Option<CompressionFormat>,
    pub ndjson_compression: Option<CompressionFormat>,
    pub parquet_compression: Option<CompressionFormat>, // Not used in UI, but kept for API compatibility
}

#[derive(Debug, Default, PartialEq, Eq)]
pub enum InputMode {
    #[default]
    Normal,
    /// The home screen: pick a dataset to open. Reachable at startup with no
    /// arguments, and from inside a session, which is what makes datui a place you
    /// stay rather than a command you re-run.
    Home,
    SortFilter,
    PivotMelt,
    Editing,
    Export,
    /// The copy dialog over the table.
    Copy,
    Info,
    Chart,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputType {
    Search,
    GoToLine,
}

/// Focus within the query prompt: the tab bar or the current mode's input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueryFocus {
    TabBar,
    #[default]
    Input,
}

#[derive(Default)]
pub struct ErrorModal {
    pub active: bool,
    pub message: String,
    /// How far a long message is scrolled; the render clamps it.
    pub scroll: usize,
}

impl ErrorModal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn show(&mut self, message: String) {
        self.active = true;
        self.message = message;
        self.scroll = 0;
    }

    pub fn hide(&mut self) {
        self.active = false;
        self.message.clear();
        self.scroll = 0;
    }
}

/// A completion flash (the Feedback rules' second rung): one plain sentence on
/// the control bar, cleared by the next keypress or after two seconds,
/// whichever comes first. Every screen's completions go here, the home screen's
/// included. The home screen's own status line beside the filter is for what a
/// key could not do and why, which has to survive until it is read.
pub struct Flash {
    pub message: String,
    expires: std::time::Instant,
}

impl Flash {
    fn new(message: String) -> Self {
        Self {
            message,
            expires: std::time::Instant::now() + std::time::Duration::from_secs(2),
        }
    }

    fn expired(&self) -> bool {
        std::time::Instant::now() >= self.expires
    }
}

pub struct ConfirmationModal {
    pub active: bool,
    pub message: String,
    pub focus_yes: bool, // true = Yes focused, false = No focused
    /// What Enter-on-Yes does, named: "Overwrite", not a generic "Yes".
    pub yes_label: &'static str,
    /// How far a long message is scrolled; the render clamps it.
    pub scroll: usize,
}

impl Default for ConfirmationModal {
    fn default() -> Self {
        Self {
            active: false,
            message: String::new(),
            focus_yes: true,
            yes_label: "Yes",
            scroll: 0,
        }
    }
}

impl ConfirmationModal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn show(&mut self, message: String) {
        self.active = true;
        self.message = message;
        self.focus_yes = true; // Default to Yes
        self.yes_label = "Yes";
        self.scroll = 0;
    }

    /// A confirmation whose Yes destroys something: it starts on No, so a
    /// reflexive second Enter declines, and the action is named on the choice.
    pub fn show_destructive(&mut self, message: String, yes_label: &'static str) {
        self.active = true;
        self.message = message;
        self.focus_yes = false;
        self.yes_label = yes_label;
        self.scroll = 0;
    }

    pub fn hide(&mut self) {
        self.active = false;
        self.message.clear();
        self.focus_yes = true;
        self.yes_label = "Yes";
        self.scroll = 0;
    }
}

/// Pending remote download; shown in confirmation modal before starting download.
#[cfg(any(feature = "http", feature = "cloud"))]
#[derive(Clone)]
pub enum PendingDownload {
    #[cfg(feature = "http")]
    Http {
        url: String,
        size: Option<u64>,
        options: OpenOptions,
    },
    #[cfg(feature = "cloud")]
    S3 {
        url: String,
        size: Option<u64>,
        options: OpenOptions,
    },
    #[cfg(feature = "cloud")]
    Gcs {
        url: String,
        size: Option<u64>,
        options: OpenOptions,
    },
    #[cfg(feature = "cloud")]
    Azure {
        url: String,
        size: Option<u64>,
        options: OpenOptions,
    },
}

#[cfg(any(feature = "http", feature = "cloud"))]
impl PendingDownload {
    /// The url, the size the probe found, and the open options — the same three
    /// fields whichever store this came from.
    fn parts(&self) -> (&str, Option<u64>, &OpenOptions) {
        match self {
            #[cfg(feature = "http")]
            PendingDownload::Http { url, size, options } => (url, *size, options),
            #[cfg(feature = "cloud")]
            PendingDownload::S3 { url, size, options } => (url, *size, options),
            #[cfg(feature = "cloud")]
            PendingDownload::Gcs { url, size, options } => (url, *size, options),
            #[cfg(feature = "cloud")]
            PendingDownload::Azure { url, size, options } => (url, *size, options),
        }
    }

    /// Replace the placeholder size with what the probe actually found.
    fn with_size(mut self, found: Option<u64>) -> Self {
        match &mut self {
            #[cfg(feature = "http")]
            PendingDownload::Http { size, .. } => *size = found,
            #[cfg(feature = "cloud")]
            PendingDownload::S3 { size, .. } => *size = found,
            #[cfg(feature = "cloud")]
            PendingDownload::Gcs { size, .. } => *size = found,
            #[cfg(feature = "cloud")]
            PendingDownload::Azure { size, .. } => *size = found,
        }
        self
    }
}

#[derive(Clone, Debug, Default)]
pub enum LoadingState {
    #[default]
    Idle,
    Loading {
        /// None when loading from LazyFrame (e.g. Python binding); Some for file paths.
        file_path: Option<PathBuf>,
        file_size: u64,        // Size of compressed file in bytes (0 when no path)
        current_phase: String, // e.g., "Scanning input", "Caching schema", "Loading buffer"
        progress_percent: u16, // 0-100
    },
    Exporting {
        file_path: PathBuf,
        current_phase: String, // e.g., "Collecting data", "Writing file", "Compressing"
        progress_percent: u16, // 0-100
    },
}

impl LoadingState {
    pub fn is_loading(&self) -> bool {
        matches!(
            self,
            LoadingState::Loading { .. } | LoadingState::Exporting { .. }
        )
    }
}

/// In-progress analysis computation state (orchestration in App; modal only displays progress).
#[allow(dead_code)]
struct AnalysisComputationState {
    df: Option<DataFrame>,
    schema: Option<Arc<Schema>>,
    partial_stats: Vec<crate::statistics::ColumnStatistics>,
    current: usize,
    total: usize,
    total_rows: usize,
    sample_seed: u64,
    sample_size: Option<usize>,
}

/// At most one query type can be active. Returns (query, sql_query, fuzzy_query) with only the
/// active one set (SQL takes precedence over fuzzy over DSL query). Used when saving template settings.
fn active_query_settings(
    dsl_query: &str,
    sql_query: &str,
    fuzzy_query: &str,
) -> (Option<String>, Option<String>, Option<String>) {
    let sql_trimmed = sql_query.trim();
    let fuzzy_trimmed = fuzzy_query.trim();
    let dsl_trimmed = dsl_query.trim();
    if !sql_trimmed.is_empty() {
        (None, Some(sql_trimmed.to_string()), None)
    } else if !fuzzy_trimmed.is_empty() {
        (None, None, Some(fuzzy_trimmed.to_string()))
    } else if !dsl_trimmed.is_empty() {
        (Some(dsl_trimmed.to_string()), None, None)
    } else {
        (None, None, None)
    }
}

// Helper struct to save state before template application
struct TemplateApplicationState {
    lf: LazyFrame,
    base_lf: LazyFrame,
    reshaped_lf: Option<LazyFrame>,
    pivot: Option<PivotSpec>,
    melt: Option<MeltSpec>,
    schema: Arc<Schema>,
    active_query: String,
    active_sql_query: String,
    active_fuzzy_query: String,
    filters: Vec<FilterStatement>,
    sort_columns: Vec<String>,
    sort_descending: Vec<bool>,
    sort_ascending: bool,
    column_order: Vec<String>,
    locked_columns_count: usize,
    /// Whether `lf` carries the hidden drift column, and what its groups mean. Rolling
    /// the frame back without these would leave the two disagreeing.
    drift: bool,
    drift_groups: Arc<Vec<crate::schema_union::DriftGroup>>,
    notes: Vec<crate::notes::Note>,
    /// Whether the notes had already been offered. A rollback is not news, so the
    /// quiet accent on `i` should be where the user left it afterwards.
    notes_seen: bool,
}

/// Outcomes of chart preparation keyed by the request that produced them, least
/// recently used first. A failure is remembered too, so a selection that cannot be
/// charted is not retried after every event; it draws as empty, as it always has.
/// Bounded so that toggling between a few selections does not collect again, without
/// holding every series ever prepared: XY series are the only payload that grows with
/// the row limit, so few of those are kept and only the current one has its log copy.
#[derive(Default)]
pub(crate) struct ChartCache {
    entries: Vec<(ChartRequest, Result<ChartPrepared, String>)>,
}

impl ChartCache {
    const CAPACITY: usize = 8;
    const XY_CAPACITY: usize = 2;

    fn clear(&mut self) {
        self.entries.clear();
    }

    fn get(&self, request: &ChartRequest) -> Option<&Result<ChartPrepared, String>> {
        self.entries
            .iter()
            .find(|(r, _)| r == request)
            .map(|(_, outcome)| outcome)
    }

    /// The prepared data for `request`, if it has been prepared.
    pub(crate) fn prepared(&self, request: &ChartRequest) -> Option<&ChartPrepared> {
        self.get(request).and_then(|outcome| outcome.as_ref().ok())
    }

    /// Whether `request` has been prepared.
    fn satisfies(&self, request: &ChartRequest) -> bool {
        self.prepared(request).is_some()
    }

    fn insert(&mut self, request: ChartRequest, outcome: Result<ChartPrepared, String>) {
        self.entries.retain(|(r, _)| *r != request);
        self.entries.push((request, outcome));
        Self::evict(&mut self.entries, Self::XY_CAPACITY, |outcome| {
            matches!(outcome, Ok(ChartPrepared::XY(_)))
        });
        Self::evict(&mut self.entries, Self::CAPACITY, |_| true);
    }

    /// Drop the least recently used of the entries `counts` selects until at most `cap`
    /// remain.
    fn evict(
        entries: &mut Vec<(ChartRequest, Result<ChartPrepared, String>)>,
        cap: usize,
        counts: impl Fn(&Result<ChartPrepared, String>) -> bool,
    ) {
        let mut over = entries
            .iter()
            .filter(|(_, o)| counts(o))
            .count()
            .saturating_sub(cap);
        entries.retain(|(_, o)| {
            if over > 0 && counts(o) {
                over -= 1;
                false
            } else {
                true
            }
        });
    }

    /// Note that `request` is the selection on screen: its entry moves to the back,
    /// where eviction reaches it last, and it alone keeps a log-scale copy of its XY
    /// series, built here when wanted. A pure in-memory map, cheap enough for the event
    /// thread; it never happens in render.
    fn touch(&mut self, request: &ChartRequest, log_scale: bool) {
        let Some(i) = self.entries.iter().position(|(r, _)| r == request) else {
            return;
        };
        let current = self.entries.remove(i);
        self.entries.push(current);
        let Some(((_, current), others)) = self.entries.split_last_mut() else {
            return;
        };
        for (_, outcome) in others {
            if let Ok(ChartPrepared::XY(xy)) = outcome {
                xy.series_log = None;
            }
        }
        if let Ok(ChartPrepared::XY(xy)) = current
            && log_scale
            && xy.series_log.is_none()
        {
            xy.series_log = Some(log_series(&xy.series));
        }
    }
}

fn log_series(series: &[Vec<(f64, f64)>]) -> Vec<Vec<(f64, f64)>> {
    series
        .iter()
        .map(|pts| pts.iter().map(|&(x, y)| (x, y.max(0.0).ln_1p())).collect())
        .collect()
}

/// What the chart view needs prepared for the modal's current selection. Compared with
/// the cache and with the computation in flight, so each selection is prepared once, off
/// the UI thread, and a result for a selection the user has since moved past is stale.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ChartRequest {
    XY {
        x_column: String,
        y_columns: Vec<String>,
        row_limit: Option<usize>,
    },
    /// Only an x column is selected: its range gives the placeholder axis its bounds.
    XRange {
        x_column: String,
        row_limit: Option<usize>,
    },
    Histogram {
        column: String,
        bins: usize,
        row_limit: Option<usize>,
    },
    BoxPlot {
        column: String,
        row_limit: Option<usize>,
    },
    Kde {
        column: String,
        bandwidth_factor: f64,
        row_limit: Option<usize>,
    },
    Heatmap {
        x_column: String,
        y_column: String,
        bins: usize,
        row_limit: Option<usize>,
    },
}

impl ChartRequest {
    fn from_modal(modal: &ChartModal) -> Option<Self> {
        let row_limit = modal.row_limit;
        match modal.chart_kind {
            ChartKind::XY => {
                let x_column = modal.effective_x_column()?.clone();
                let y_columns = modal.effective_y_columns();
                Some(if y_columns.is_empty() {
                    Self::XRange {
                        x_column,
                        row_limit,
                    }
                } else {
                    Self::XY {
                        x_column,
                        y_columns,
                        row_limit,
                    }
                })
            }
            ChartKind::Histogram => Some(Self::Histogram {
                column: modal.effective_hist_column()?,
                bins: modal.hist_bins,
                row_limit,
            }),
            ChartKind::BoxPlot => Some(Self::BoxPlot {
                column: modal.effective_box_column()?,
                row_limit,
            }),
            ChartKind::Kde => Some(Self::Kde {
                column: modal.effective_kde_column()?,
                bandwidth_factor: modal.kde_bandwidth_factor,
                row_limit,
            }),
            ChartKind::Heatmap => Some(Self::Heatmap {
                x_column: modal.effective_heatmap_x_column()?,
                y_column: modal.effective_heatmap_y_column()?,
                bins: modal.heatmap_bins,
                row_limit,
            }),
        }
    }

    /// The Polars work. Runs on a worker thread; `rows` is the effective row limit.
    fn prepare(&self, lf: &LazyFrame, schema: &Schema, rows: usize) -> Result<ChartPrepared> {
        Ok(match self {
            Self::XY {
                x_column,
                y_columns,
                ..
            } => {
                let r = chart_data::prepare_chart_data(lf, schema, x_column, y_columns, rows)?;
                ChartPrepared::XY(ChartCacheXY {
                    x_column: x_column.clone(),
                    y_columns: y_columns.clone(),
                    series: r.series,
                    series_log: None,
                    x_axis_kind: r.x_axis_kind,
                })
            }
            Self::XRange { x_column, .. } => ChartPrepared::XRange(
                chart_data::prepare_chart_x_range(lf, schema, x_column, rows)?,
            ),
            Self::Histogram { column, bins, .. } => ChartPrepared::Histogram(
                chart_data::prepare_histogram_data(lf, column, *bins, rows)?,
            ),
            Self::BoxPlot { column, .. } => ChartPrepared::BoxPlot(
                chart_data::prepare_box_plot_data(lf, std::slice::from_ref(column), rows)?,
            ),
            Self::Kde {
                column,
                bandwidth_factor,
                ..
            } => ChartPrepared::Kde(chart_data::prepare_kde_data(
                lf,
                std::slice::from_ref(column),
                *bandwidth_factor,
                rows,
            )?),
            Self::Heatmap {
                x_column,
                y_column,
                bins,
                ..
            } => ChartPrepared::Heatmap(chart_data::prepare_heatmap_data(
                lf, x_column, y_column, *bins, rows,
            )?),
        })
    }
}

/// The outcome handed from the chart worker to `BackgroundChartReady`.
type ChartResultSlot = Arc<Mutex<Option<Result<ChartPrepared, String>>>>;

/// The chart preparation currently running. There is at most one: a burst of selection
/// changes must not fan out into a full collect per column, so the next request waits
/// for this one to land and then the newest selection is the one prepared. Being the
/// only one is also what ties a `BackgroundChartReady` to it, so no generation is
/// needed to match them up.
struct ChartInflight {
    /// `len_generation` of the dataset the request was spawned against, so a result
    /// cannot be installed for a different dataset that happens to share column names.
    dataset: Option<u64>,
    request: ChartRequest,
    /// Set when the view or dataset it was spawned for has gone. The worker cannot be
    /// cancelled, so the record stays until its result lands and is discarded; the next
    /// request waits for it, which is what keeps the number of collects at one.
    stale: bool,
}

/// A prepared chart, ready to go into the cache. Each payload names the columns it
/// was drawn from, since render and export label the axes from it.
pub(crate) enum ChartPrepared {
    XY(ChartCacheXY),
    XRange(chart_data::ChartXRangeResult),
    Histogram(chart_data::HistogramData),
    BoxPlot(chart_data::BoxPlotData),
    Kde(chart_data::KdeData),
    Heatmap(chart_data::HeatmapData),
}

/// A chart export with its data taken from the cache; `write` is the slow part and runs
/// off the UI thread.
enum ChartExportJob {
    Series {
        series: Vec<ChartExportSeries>,
        chart_type: ChartType,
        bounds: ChartExportBounds,
    },
    BoxPlot {
        data: chart_data::BoxPlotData,
        bounds: BoxPlotExportBounds,
    },
    Heatmap {
        data: chart_data::HeatmapData,
        bounds: ChartExportBounds,
    },
}

impl ChartExportJob {
    fn write(&self, path: &Path, format: ChartExportFormat, size: (u32, u32)) -> Result<()> {
        match (self, format) {
            (
                Self::Series {
                    series,
                    chart_type,
                    bounds,
                },
                ChartExportFormat::Png,
            ) => write_chart_png(path, series, *chart_type, bounds, size),
            (
                Self::Series {
                    series,
                    chart_type,
                    bounds,
                },
                ChartExportFormat::Eps,
            ) => write_chart_eps(path, series, *chart_type, bounds),
            (Self::BoxPlot { data, bounds }, ChartExportFormat::Png) => {
                write_box_plot_png(path, data, bounds, size)
            }
            (Self::BoxPlot { data, bounds }, ChartExportFormat::Eps) => {
                write_box_plot_eps(path, data, bounds)
            }
            (Self::Heatmap { data, bounds }, ChartExportFormat::Png) => {
                write_heatmap_png(path, data, bounds, size)
            }
            (Self::Heatmap { data, bounds }, ChartExportFormat::Eps) => {
                write_heatmap_eps(path, data, bounds)
            }
        }
    }
}

pub(crate) struct ChartCacheXY {
    /// Axis labels; one series per y column.
    pub(crate) x_column: String,
    pub(crate) y_columns: Vec<String>,
    pub(crate) series: Vec<Vec<(f64, f64)>>,
    pub(crate) series_log: Option<Vec<Vec<(f64, f64)>>>,
    pub(crate) x_axis_kind: chart_data::XAxisTemporalKind,
}

/// The buffer collect in flight: what it will fill, and for which data.
///
/// The first frame after a load sets `visible_rows` and asks for a recollect while the
/// pre-frame collect is still running; on an object store that restarted the same
/// row-group download. A collect that already covers the view is left to land instead,
/// provided nothing has moved underneath it: the frame generation must still be the
/// one it was spawned under, and so must the data (`len_generation` changes with every
/// change to `lf`, so a filter applied while it runs plans a fresh collect).
#[derive(Clone, Copy)]
struct InflightCollect {
    /// When the request went out, and how many of the dataset's files it will read, for
    /// the Last page measurement. `files` is `None` where Polars was handed the whole
    /// scan and reads what it decides to.
    began: std::time::Instant,
    files: Option<usize>,
    generation: u64,
    dataset: u64,
    start: usize,
    end: usize,
    /// Whether anything waits on it. A load-ahead starts with nobody waiting: it sets no
    /// `busy`, says nothing, and its end touches neither. A scroll that finds its rows
    /// already on the way waits on it from then, and it ends like any collect.
    waited_on: bool,
}

impl InflightCollect {
    fn covers(&self, generation: u64, state: &DataTableState) -> bool {
        // The view ends at the data when there is less than a screen of it.
        let bound = state.num_rows_if_valid().unwrap_or(usize::MAX);
        let view_end = (state.start_row + state.visible_rows).min(bound);
        // A row group being stitched on to the buffer covers the view with it.
        let (mut start, mut end) = (self.start, self.end);
        let (held_start, held_end) = (state.buffered_start(), state.buffered_end());
        if state.stitches_buffer() && (start == held_end || end == held_start) {
            start = start.min(held_start);
            end = end.max(held_end);
        }
        self.generation == generation
            && self.dataset == state.len_generation()
            && start <= state.start_row
            && view_end <= end
    }
}

/// The exact row count of a frame, to run off the UI thread: the footer sum for a
/// pristine local Parquet hive directory or remote dataset of many files, otherwise
/// `len()`. Carries the `len_generation` it was spawned under, so a result for data since
/// changed is dropped.
struct LenCount {
    len_generation: u64,
    count_dir: Option<PathBuf>,
    files: Option<crate::widgets::datatable::FileCounter>,
    lf: LazyFrame,
    streaming: bool,
    /// The open's meter. Counting a local directory re-reads every footer, which costs
    /// what the open's own pass cost and is tallied with it.
    meter: Arc<crate::measurements::Meter>,
}

/// What a cloud open was pointed at: the URL as the user gave it, the prefix to list,
/// and the glob to keep, where they named one.
///
/// Together because they are one thought — where to look — and apart they put this
/// function's signature past the point where a reader can hold it.
#[cfg(feature = "cloud")]
struct CloudTarget<'a> {
    /// The URL as typed, which is where the bucket and scheme come from.
    full: &'a str,
    /// The literal prefix to list: the whole key, or the part of a glob before its star.
    key: String,
    /// The glob the user named, where they named one. The listing keeps only the keys
    /// it matches, so everything downstream sees a plain list of files.
    pattern: Option<&'a globset::GlobMatcher>,
}

/// A count, and for a remote dataset of many files the row groups it was summed from.
struct Counted {
    rows: usize,
    file_row_groups: Option<Vec<Vec<usize>>>,
}

impl From<usize> for Counted {
    fn from(rows: usize) -> Self {
        Counted {
            rows,
            file_row_groups: None,
        }
    }
}

impl LenCount {
    fn for_state(state: &DataTableState) -> Self {
        Self {
            len_generation: state.len_generation(),
            count_dir: state.parquet_count_dir(),
            files: state.remote_files_counter(),
            meter: state.measurements().clone(),
            lf: state.lf_clone(),
            streaming: state.polars_streaming_enabled(),
        }
    }

    /// Whether this count reads only footers, and so can run beside a buffer read
    /// rather than waiting for it.
    fn reads_footers(&self) -> bool {
        self.files.is_some() || self.count_dir.is_some()
    }

    /// Count the rows. Blocks; `Err` when the count could not be taken.
    fn run(&self) -> Result<Counted, ()> {
        // A dataset's footers, many at once. Should one not read, the scan counts itself.
        if let Some(count) = &self.files
            && let Ok(groups) = count()
        {
            return Ok(Counted {
                rows: groups.iter().flatten().sum(),
                file_row_groups: Some(groups),
            });
        }
        match &self.count_dir {
            Some(dir) => DataTableState::count_rows_from_parquet_dir(dir, &self.meter)
                .map(Counted::from)
                .map_err(|_| ()),
            None => {
                match crate::statistics::collect_lazy(
                    crate::widgets::datatable::row_count_lf(&self.lf),
                    self.streaming,
                ) {
                    Ok(df) => Ok(match df.get(0) {
                        Some(col) => match col.first() {
                            Some(AnyValue::UInt64(n)) => *n as usize,
                            _ => 0,
                        },
                        None => 0,
                    }
                    .into()),
                    Err(_) => Err(()),
                }
            }
        }
    }

    /// The count once a buffer collect of `requested` rows from `start` has returned
    /// `returned` of them. A short read that began inside the data — at its top, or
    /// finding at least a row — ran off its end, which names the total without a pass
    /// over it. A full read, or a slice deep in a frame that found nothing and may lie
    /// past the data entirely, leaves the count to `run`.
    fn after_collect(
        &self,
        start: usize,
        returned: usize,
        requested: usize,
    ) -> Result<Counted, ()> {
        if returned < requested && (start == 0 || returned > 0) {
            Ok((start + returned).into())
        } else {
            self.run()
        }
    }

    /// Report the count. A failure leaves the total provisional and allows a retry on a
    /// later interaction; the buffer paint is unaffected either way.
    fn send(&self, counted: Result<Counted, ()>, tx: &Sender<AppEvent>) {
        let _ = tx.send(match counted {
            Ok(counted) => AppEvent::BackgroundLenReady {
                len_generation: self.len_generation,
                num_rows: counted.rows,
                file_row_groups: counted.file_row_groups,
            },
            Err(()) => AppEvent::BackgroundLenFailed {
                len_generation: self.len_generation,
            },
        });
    }
}

struct QualityCacheEntry {
    dataset_generation: u64,
    view_generation: u64,
    plan: data_quality::DataQualityPlan,
    results: data_quality::DataQualityResults,
}

/// The rows the last sampled Data Quality run read, and what decided which rows they
/// were. A run or a drill that names the same rows cuts these instead of reading.
#[derive(Debug, Clone)]
pub struct KeptQualitySample {
    dataset_generation: u64,
    view_generation: u64,
    sample: sampling::Sample,
    rows: std::sync::Arc<data_quality::QualitySample>,
}

pub struct App {
    pub data_table_state: Option<DataTableState>,
    /// How far the footer pass of an open has got. Written by the threads reading
    /// them; read once a frame into [`Self::footers_this_frame`], which is what the
    /// loading screen and the control bar actually show.
    pub footer_progress: Arc<crate::schema_union::FooterProgress>,
    /// The count as it stood when this frame began, or `None` if no pass was running.
    ///
    /// Taken once because the pass is running on other threads while the frame is
    /// drawn. The loading body and the control bar are painted a millisecond apart,
    /// and when each read the counter for itself they printed different numbers for
    /// one wait — and the bar could print a phase's flat percentage beside a count
    /// that had finished between the two reads.
    footers_this_frame: Option<(usize, usize)>,
    /// Network roots currently being listed off-thread, so a probe is not started
    /// twice. Entries are never removed for a root that never answers — that thread
    /// is unreclaimable, and retrying it would only block another one.
    home_probes_inflight: Vec<PathBuf>,
    /// True once cloud discovery has been started. Enumeration costs a request per
    /// provider, so it happens once and its result is kept for the session.
    #[cfg(feature = "cloud")]
    cloud_discovery_started: bool,
    /// True while a recursive search below the working directory is out. One at a
    /// time: the walk is bounded, and a second one would only compete for the disk.
    home_search_inflight: bool,
    /// Set while the confirmation modal is asking about forgetting every recent.
    pending_clear_recents: bool,
    /// The place whose recents the confirmation modal is asking about forgetting.
    pending_forget_place: Option<PathBuf>,
    /// Why the last open failed, shown on the home screen when the error is dismissed
    /// and there is nothing to fall back to.
    last_load_error: Option<String>,
    /// Schema reads currently out, so the same one is not requested every frame.
    home_schema_inflight: Vec<PathBuf>,
    /// Invalidates listings and measurements from a request the user has moved past.
    home_generation: u64,
    /// The look a `ClassifyThenOpen` has out, if any. Every key acts on the home screen
    /// even while `busy` — `hard_escape_while_busy` says so there — so a second Enter is
    /// reachable, and the newer look replaces the older: its answer is the one the user
    /// is waiting for. See [`ClassifyRequest`].
    classify_inflight: Option<ClassifyRequest>,
    /// Ids for those, so a superseded answer can be told from the one being waited on.
    classify_requests: u64,
    /// The directory a `LookThenOpenDirectory` is being looked at, if any.
    ///
    /// The look takes seconds on a directory of large Parquet, and Ctrl+O works
    /// throughout — that is the point of it being off the startup thread — so the user
    /// can be somewhere else by the time it answers. `abandon_load` puts it down with
    /// everything else that belonged to the screen being left, and an answer that finds
    /// nothing outstanding touches nothing. Without it the look landed seventeen seconds
    /// later and took the user off the home screen they had chosen.
    ///
    /// Its own field rather than `task_generation`, which `abandon_load` deliberately
    /// does not bump: a load abandoned is not a newer load, and bumping it there would
    /// discard answers that other waiting work still wants.
    looking_at_directory: Option<PathBuf>,
    /// Home screen state. Rebuilt from the filesystem whenever home is entered;
    /// nothing here is persisted beyond the recents list.
    pub home: home::HomeState,
    /// Schema previews, memoised for the session only. Persisting these would be a
    /// catalogue by another name, and it would go stale.
    home_schema_cache: HashMap<PathBuf, Option<discover::SchemaPreview>>,
    path: Option<PathBuf>,
    original_file_format: Option<ExportFormat>, // Track original file format for default export
    original_file_delimiter: Option<u8>, // Track original file delimiter for CSV export default
    events: Sender<AppEvent>,
    debug: DebugState,
    pub info_modal: InfoModal,
    parquet_metadata_cache: Option<ParquetMetadataCache>,
    // One input per query mode, each with its own history. The history ids
    // ("query", "sql", "fuzzy") name files already on disk; they stay as they
    // are so no history is lost or read as another mode's.
    query_input: TextInput, // q-style, history id "query"; also borrowed by go-to-line
    sql_input: TextInput,   // SQL, history id "sql"
    fuzzy_input: TextInput, // Search, history id "fuzzy"
    pub input_mode: InputMode,
    input_type: Option<InputType>,
    query_mode: QueryMode,
    query_focus: QueryFocus,
    pub sort_filter_modal: SortFilterModal,
    pub pivot_melt_modal: PivotMeltModal,
    pub template_modal: TemplateModal,
    /// Whether the open dataset was reached through the home screen. `q` pops
    /// the context: opened from home it returns there, launched straight onto
    /// a file it quits — the user's mental stack, not a mode.
    opened_from_home: bool,
    /// `--template NAME`, waiting for the dataset from the command line to land.
    /// Taken on the first install, so datasets opened later are not re-dressed.
    startup_template: Option<String>,
    pub analysis_modal: AnalysisModal,
    quality_cache: Vec<QualityCacheEntry>,
    /// See [`KeptQualitySample`]. One, the last: a sample is up to two million rows.
    quality_sample: Option<KeptQualitySample>,
    /// The table an analysis drill left behind: Data Quality's matching rows or the
    /// sample's, shown in its place until Esc brings it back.
    quality_evidence_return: Option<Box<DataTableState>>,
    /// The shared sample is being read to show as a table; a cancel leaves the tool
    /// on screen as it was.
    reading_sample: bool,
    pub(crate) quality_evidence_label: Option<String>,
    pub chart_modal: ChartModal,
    pub chart_export_modal: ChartExportModal,
    pub export_modal: ExportModal,
    pub copy_modal: copy_modal::CopyModal,
    /// Where copies go. Built at the first copy and kept for the run: on
    /// Wayland and X11 the clipboard offer dies with the process that owns it,
    /// so this handle must live as long as the copy should.
    clipboard: Option<Box<dyn clipboard::Destination>>,
    /// A table-scope copy waiting on the size confirmation.
    pending_copy: Option<(clipboard::CopyFormat, bool)>,
    pub(crate) chart_cache: ChartCache,
    /// The one chart preparation allowed to run at a time. Render draws only what is in
    /// `chart_cache`; this drives the throbber while it is current. Its result is
    /// installed only if the record is still current (not `stale`) and the dataset is
    /// the one it was computed from. Deliberately not
    /// `busy`: the sidebar stays live while the data is computed, and the newest
    /// selection is prepared once this one lands.
    chart_inflight: Option<ChartInflight>,
    /// Generation and in-flight marker of the chart export write. Separate from
    /// `task_generation`, which going home deliberately leaves alone (it also gates
    /// data exports and analysis); leaving the dataset drops this one instead.
    chart_export_generation: u64,
    chart_export_inflight: Option<u64>,
    /// The result of the background chart preparation, like `pending_collect_result`:
    /// the data stays out of the event.
    pending_chart_result: ChartResultSlot,
    /// A chart export that asked for data still being prepared. `BackgroundChartReady`
    /// picks it up; `busy` stays set until then.
    chart_export_waiting: Option<(PathBuf, ChartExportFormat, String, u32, u32)>,
    error_modal: ErrorModal,
    flash: Option<Flash>,
    pub confirmation_modal: ConfirmationModal,
    pending_export: Option<(PathBuf, ExportFormat, ExportOptions)>, // Store export request while waiting for confirmation
    /// Collected DataFrame between DoExportCollect and DoExportWrite (two-phase export progress).
    export_df: Option<DataFrame>,
    pending_chart_export: Option<(PathBuf, ChartExportFormat, String, u32, u32)>,
    /// Pending remote file download (HTTP/S3/GCS) while waiting for user confirmation.
    /// Size is from HEAD when available.
    ///
    /// Carries a [`GenerationLease`], because this is the one errand that waits on
    /// neither a worker nor a continuation: nothing is running, the open is very much
    /// unfinished, and the wait is as long as the user takes. Paired with the download
    /// rather than kept beside it, so the two cannot drift — every path out of the modal
    /// takes the download, and the lease goes with it.
    #[cfg(any(feature = "http", feature = "cloud"))]
    pending_download: Option<(PendingDownload, GenerationLease)>,
    show_help: bool,
    help_scroll: usize, // Scroll position for help content
    cache: CacheManager,
    template_manager: TemplateManager,
    active_template_id: Option<String>, // ID of currently applied template
    loading_state: LoadingState,        // Current loading state for progress indication
    theme: Theme,                       // Color theme for UI rendering
    /// `a` is waiting on the confirmation to read every row.
    pending_read_all: bool,
    history_limit: usize, // History limit for all text inputs (from config.query.history_limit)
    table_cell_padding: u16, // Spaces between columns (from config.display.table_cell_padding)
    column_colors: bool, // When true, colorize table cells by column type (from config.display.column_colors)
    /// Second header row of column types. Starts from `display.dtype_row`; `D` flips it.
    dtype_row: bool,
    // Resolved display-time number formatting. `enabled` is flipped by the F key.
    number_format: NumberFormatSettings,
    runtime: tokio::runtime::Handle, // Tokio runtime handle for background tasks
    task_generation: u64,            // Incremented to invalidate stale background results
    /// True while the load started by the most recent `Open`/`OpenLazyFrame` is still
    /// wanted. Going home clears it, which is what abandons an in-flight load: the
    /// remaining `Do*` chain events and the results that would install a dataset all
    /// check this and bail. Deliberately separate from `task_generation`, which also
    /// gates analysis and export results — going home must not cancel an export.
    load_active: bool,
    /// True from the moment a load starts until it installs its dataset, fails, or is
    /// abandoned. While it is set, whatever `data_table_state` holds belongs to the
    /// *previous* dataset, so the main view shows the load's progress instead of it —
    /// otherwise the old table sits under the new file's name for the whole load.
    ///
    /// Separate from `load_active`, which stays set through the buffer collect that
    /// follows installation: by then the table on screen is the right one.
    awaiting_dataset: bool,
    /// Whether the load in flight was chosen on the home screen, which is where its
    /// failure is reported: the dataset left over from before is not what the user
    /// was looking at when they chose.
    load_from_home: bool,
    /// The path an open was asked for, recorded as a recent when its dataset installs.
    recent_on_install: Option<PathBuf>,
    /// The paths an open was asked for, kept with the options it installed with once it
    /// does: what `H` opens again with its header turned the other way.
    opening: Option<Vec<PathBuf>>,
    opened: Option<(Vec<PathBuf>, OpenOptions)>,
    /// The URL `http_temp_path` was downloaded from. Opening it again reads that copy
    /// rather than downloading it again, which is how `H` re-reads a downloaded file.
    #[cfg(any(feature = "http", feature = "cloud"))]
    downloaded_from: Option<PathBuf>,
    /// Where the last load-ahead was asked from. See [`App::load_ahead`].
    loaded_ahead_from: Option<(u64, usize, usize, usize)>,
    /// LazyFrame produced by a background scan, tagged with the generation that
    /// asked for it. Mirrors `pending_schema_result`; a stale entry is discarded.
    pending_lazyframe_result: Arc<Mutex<Option<(u64, LazyFrame)>>>,
    // `len_generation` of the in-flight background row-count, if any. Prevents re-spawning
    // the (potentially minutes-long) count on every scroll while it's still running.
    len_count_inflight: Option<u64>,
    // `len_generation` whose background row-count failed. While this matches the current
    // generation (and the count is still invalid) the row count is shown as "?" rather than a
    // misleading provisional total.
    len_count_failed: Option<u64>,
    /// End was pressed on a remote dataset before its rows were counted: go there when
    /// the count for this generation arrives, rather than to a guess.
    end_after_count: Option<u64>,
    /// The buffer collect in flight, if any. See [`InflightCollect`].
    collect_inflight: Option<InflightCollect>,
    pending_schema_result: std::sync::Arc<std::sync::Mutex<Option<(u64, DataTableState)>>>, // (generation, result) from background schema load
    /// What the pass behind a staged open found, for the frame that applies it. Mirrors
    /// `pending_schema_result`: large enough to be worth keeping out of the event, and
    /// discarded if the dataset it belongs to has been replaced.
    pending_footers_result: std::sync::Arc<std::sync::Mutex<FootersReported>>,
    /// Bumped once per dataset put on screen, which `task_generation` is not: a collect
    /// bumps that, and the pass reading the rest of a dataset's footers outlives
    /// several. It is what says whether the columns arriving belong to the dataset the
    /// user is looking at.
    dataset_generation: u64,
    /// End was pressed while a dataset was still reading its footers, which is where
    /// its end is coming from. Jump when they land — and only for that dataset, which
    /// is what the generation is for: a directory the user pressed End on and then walked
    /// away from must not move the view of the one they opened next. `end_after_count`
    /// alongside keys itself the same way, to `len_generation`.
    end_when_the_footers_land: Option<u64>,
    /// What a dataset's footers found while the user was looking at a query, a pivot or
    /// a drill-down rather than at the data. Held rather than applied, because widening
    /// the scan under a query takes the query's own columns away, and offered again the
    /// moment the view comes back to the dataset itself.
    footers_held: Option<(u64, crate::widgets::datatable::FootersFound)>,
    /// A re-read the dataset is owed by a footer pass that came back empty-handed, held
    /// back because the collect it goes through would bump `task_generation` out from
    /// under work already running. The pass that failed brings no columns to hold, so
    /// `footers_held` has nothing to say about it, and the dataset still needs the
    /// ordinary count the pass was going to save it — hence an errand of its own, tried
    /// again after every event until the work it would cancel is done.
    reread_owed: Option<u64>,
    /// Leases outstanding, by the generation each was taken on. Those on the current
    /// `task_generation` are background work a bump would strand. See
    /// [`GenerationLease`].
    leases: HashMap<u64, usize>,
    /// A buffer collect that was asked for while a lease was outstanding, and the
    /// dataset it was asked for. Tried again after every event, like `reread_owed`, and
    /// dropped when the dataset it belonged to is replaced.
    collect_owed: Option<(u64, String)>,
    pending_collect_result:
        std::sync::Arc<std::sync::Mutex<Option<(u64, crate::widgets::datatable::CollectResult)>>>, // (generation, result) from background buffer load
    /// When true, show the throbber and defer keys (see [`App::handle`]); the main loop
    /// holds them until this clears.
    busy: bool,
    /// Bumped whenever the screen the user was typing at is replaced without a key of
    /// theirs asking for it: going home, abandoning a load. Keys held while busy carry
    /// the value they were typed under and are dropped if it has moved on.
    screen_generation: u64,
    /// Set by the main loop when it had to drop a key typed while busy, shown beside a
    /// status message while work is running. Cleared once the held keys have been
    /// replayed.
    input_dropped: bool,
    throbber_frame: u8, // Spinner frame index (0..3) for control bar
    /// Status text for the control bar, at the table view. Shown whether or not the app
    /// is busy: an End waiting on a remote row count parks without setting `busy`.
    status_message: Option<String>,
    analysis_computation: Option<AnalysisComputationState>,
    app_config: AppConfig,
    /// Temp file path for HTTP-downloaded data; removed when user opens different data or exits.
    // Gated to match the events that write it, below. A cloud object is downloaded to
    // a temp file exactly as an HTTP URL is, so a build with `cloud` but not `http`
    // still needs somewhere to record the file and still has to delete it.
    #[cfg(any(feature = "http", feature = "cloud"))]
    http_temp_path: Option<PathBuf>,
}

impl App {
    fn open_quality_evidence(&mut self) -> Option<AppEvent> {
        let results = self.analysis_modal.data_quality_results.as_ref()?;
        let report = quality_report::build_report(results);
        // Files named from the footers open from the table whatever the run read;
        // a sampled measurement opens the sample it was taken on.
        let finding = self
            .analysis_modal
            .data_quality_table_state
            .selected()
            .and_then(|index| report.findings.get(index))
            .filter(|finding| finding.can_open_rows(results))?;
        // A column its file never had, or holds in a type the scan cannot read, has no
        // value to filter on: its rows are the ones those files contributed, which is a
        // scope rather than a predicate.
        let by_files = finding.evidence_scope(results);
        let predicate = match (&by_files, finding.evidence_predicate(results)) {
            (Some(_), _) => polars::prelude::lit(true),
            (None, Some(predicate)) => predicate,
            (None, None) => return None,
        };
        let label = format!(
            "Data Quality / {} / {}",
            finding.title,
            quality_report::columns_label(&finding.columns, 40)
        );
        if finding.opens_sample(results) {
            let sample = self
                .analysis_modal
                .data_quality_last_plan
                .as_ref()
                .unwrap_or(&self.analysis_modal.data_quality_plan)
                .sample();
            return self.read_sample_rows(sample, Some((predicate, format!("{label} / sampled"))));
        }
        let state = self.data_table_state.as_ref()?;
        let scope = by_files.as_ref().unwrap_or_else(|| {
            self.analysis_modal
                .data_quality_last_plan
                .as_ref()
                .map(|plan| &plan.scope)
                .unwrap_or(&self.analysis_modal.data_quality_plan.scope)
        });
        let view = match state.quality_evidence_view(scope, predicate) {
            Ok(view) => view,
            Err(error) => {
                self.error_modal
                    .show(format!("Cannot open matching rows: {error}"));
                return None;
            }
        };
        if let Some(original) = self.data_table_state.replace(view) {
            self.quality_evidence_return = Some(Box::new(original));
            self.quality_evidence_label = Some(label);
            self.analysis_modal.active = false;
            self.collect_inflight = None;
            self.spawn_async_collect("Loading matching rows...");
        }
        None
    }

    fn return_from_quality_evidence(&mut self, reopen_analysis: bool) -> bool {
        let Some(original) = self.quality_evidence_return.take() else {
            return false;
        };
        self.task_generation = self.task_generation.wrapping_add(1);
        self.collect_inflight = None;
        self.len_count_inflight = None;
        self.data_table_state = Some(*original);
        self.quality_evidence_label = None;
        self.analysis_modal.active = reopen_analysis;
        self.busy = false;
        self.status_message = None;
        true
    }

    fn restore_recent_quality_plan(&mut self) {
        let Some(view_generation) = self
            .data_table_state
            .as_ref()
            .map(DataTableState::len_generation)
        else {
            return;
        };
        if self.analysis_modal.data_quality_plan != data_quality::DataQualityPlan::default() {
            return;
        }
        if let Some(cached) = self.quality_cache.iter().find(|entry| {
            entry.dataset_generation == self.dataset_generation
                && entry.view_generation == view_generation
        }) {
            self.analysis_modal.data_quality_plan = cached.plan.clone();
        }
    }

    /// What the data offers the plan's choices.
    fn quality_plan_context(&self) -> analysis_modal::PlanContext {
        let Some(state) = self.data_table_state.as_ref() else {
            return analysis_modal::PlanContext::default();
        };
        let scope = &self.analysis_modal.data_quality_plan.scope;
        let schema = &state.schema;
        let mut partitions = state.partition_columns.clone().unwrap_or_default();
        // A directory whose files agree opens as one scan and names no partition
        // columns; its directory names still do.
        if partitions.is_empty()
            && let Some(dir) = self.path.as_ref().filter(|path| path.is_dir())
        {
            partitions = DataTableState::discover_hive_partition_columns(dir)
                .into_iter()
                .filter(|column| schema.get(column).is_some())
                .collect();
        }
        analysis_modal::PlanContext {
            partitions,
            time_columns: state
                .quality_temporal_columns(scope)
                .into_iter()
                .map(|column| {
                    let has_time =
                        !matches!(schema.get(&column), Some(polars::prelude::DataType::Date));
                    (column, has_time)
                })
                .collect(),
            files: state.quality_source_file_count() > 1,
        }
    }

    /// Space on a plan field: the Sample form, the role editor, or the field's
    /// choices.
    fn open_plan_field(&mut self) -> Option<AppEvent> {
        match self.analysis_modal.data_quality_plan_field {
            0 => self.open_sample_form(),
            4 => {
                // With no date or time column there is no role to assign.
                if self.has_quality_time_columns() {
                    self.analysis_modal.data_quality_plan_before_edit =
                        Some(self.analysis_modal.data_quality_plan.clone());
                    self.analysis_modal
                        .set_quality_page(data_quality::QualityPage::TimeRoles);
                    self.analysis_modal.data_quality_plan_field = 0;
                }
            }
            field => {
                let context = self.quality_plan_context();
                self.analysis_modal.open_plan_picker(field, &context);
            }
        }
        None
    }

    /// The plan setting the Data Quality page on screen lacks before it can show
    /// anything; Enter opens it, and the control bar says so.
    pub(crate) fn quality_page_setup(&self) -> Option<data_quality::QualitySetup> {
        let modal = &self.analysis_modal;
        data_quality::page_setup(
            modal.data_quality_page,
            modal.quality_result_plan(),
            modal.data_quality_results.as_ref(),
            self.has_quality_time_columns(),
        )
    }

    /// Whether the plan's scope has a date or time column to give a role.
    pub(crate) fn has_quality_time_columns(&self) -> bool {
        self.data_table_state.as_ref().is_some_and(|state| {
            !state
                .quality_temporal_columns(&self.analysis_modal.data_quality_plan.scope)
                .is_empty()
        })
    }

    /// The rows the last sampled Data Quality run read, when they are the rows
    /// `sample` names now: same dataset, same view, same sample.
    fn kept_quality_sample(
        &self,
        sample: &sampling::Sample,
    ) -> Option<std::sync::Arc<data_quality::QualitySample>> {
        let kept = self.quality_sample.as_ref()?;
        let view_generation = self.data_table_state.as_ref()?.len_generation();
        (kept.dataset_generation == self.dataset_generation
            && kept.view_generation == view_generation
            && &kept.sample == sample)
            .then(|| kept.rows.clone())
    }

    fn restore_cached_quality(&mut self) -> bool {
        let Some(view_generation) = self
            .data_table_state
            .as_ref()
            .map(DataTableState::len_generation)
        else {
            return false;
        };
        let plan = &self.analysis_modal.data_quality_plan;
        let Some(cached) = self.quality_cache.iter().find(|entry| {
            entry.dataset_generation == self.dataset_generation
                && entry.view_generation == view_generation
                && &entry.plan == plan
        }) else {
            return false;
        };
        self.analysis_modal.data_quality_results = Some(cached.results.clone());
        self.analysis_modal.data_quality_last_plan = Some(plan.clone());
        self.analysis_modal.data_quality_from_cache = true;
        self.analysis_modal
            .set_quality_page(data_quality::QualityPage::Overview);
        true
    }

    fn cache_quality_result(
        &mut self,
        results: &data_quality::DataQualityResults,
        plan: data_quality::DataQualityPlan,
    ) {
        let Some(view_generation) = self
            .data_table_state
            .as_ref()
            .map(DataTableState::len_generation)
        else {
            return;
        };
        self.quality_cache.retain(|entry| {
            !(entry.dataset_generation == self.dataset_generation
                && entry.view_generation == view_generation
                && entry.plan == plan)
        });
        self.quality_cache.insert(
            0,
            QualityCacheEntry {
                dataset_generation: self.dataset_generation,
                view_generation,
                plan,
                results: results.clone(),
            },
        );
        self.quality_cache.truncate(4);
    }

    /// Returns true when the app is busy (background work in progress).
    pub fn is_busy(&self) -> bool {
        self.busy
    }

    /// Current background-task generation. Bumped each time work is spawned that should
    /// invalidate prior in-flight tasks. Exposed for tests that need to construct
    /// synthetic Background* events with a known-stale generation.
    pub fn task_generation(&self) -> u64 {
        self.task_generation
    }

    /// Path of the dataset currently installed, if any. Exposed for tests that need to
    /// assert an abandoned load did not swap a dataset in after the fact.
    pub fn open_path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Whether any leased background work, current or abandoned, has yet to report
    /// back. Exposed for tests that wait for abandoned work to finish rather than
    /// guessing how long it takes.
    pub fn background_work_in_flight(&self) -> bool {
        self.leases.values().any(|n| *n > 0)
    }

    /// See the `screen_generation` field.
    pub fn screen_generation(&self) -> u64 {
        self.screen_generation
    }

    /// True while a message is in front of the user that has to be dismissed.
    pub fn modal_showing(&self) -> bool {
        self.error_modal.active || self.confirmation_modal.active
    }

    /// The analysis on screen was run on a sample: what `r` and `a` act on.
    fn analysis_results_are_sampled(&self) -> bool {
        self.analysis_modal.view == analysis_modal::AnalysisView::Main
            && self.analysis_modal.computing.is_none()
            && self
                .analysis_modal
                .current_results()
                .is_some_and(|r| r.sample_size.is_some())
    }

    /// Work a cancel passed that is still running: leased on a generation since left.
    fn cancelled_work_running(&self) -> bool {
        self.leases
            .iter()
            .any(|(generation, n)| *generation != self.task_generation && *n > 0)
    }

    /// Where the value tools (Describe, Distribution, Correlation) read the shared
    /// sample from, and what the table already knows of its size. Row ranges are
    /// counted in the order the table shows; every other view scope reads without the
    /// sort, which no statistic needs and which makes a sampled read read everything.
    fn sample_source(&self, state: &DataTableState) -> (sampling::SampleSource, Option<usize>) {
        Self::sample_source_for(state, &self.analysis_modal.sample.scope)
    }

    fn sample_source_for(
        state: &DataTableState,
        scope: &data_quality::QualityScope,
    ) -> (sampling::SampleSource, Option<usize>) {
        if scope.uses_source() {
            let (lf, source) = state.data_quality_source_scan();
            return (sampling::SampleSource::loaded(lf, source), None);
        }
        let lf = match scope {
            data_quality::QualityScope::FirstRows(_)
            | data_quality::QualityScope::ViewRows { .. } => state.lf.clone(),
            _ => state.analysis_lf(),
        };
        (
            sampling::SampleSource::view(lf.select(state.binary_stub_exprs())),
            sampling::view_scope_rows(state.num_rows_if_valid(), scope),
        )
    }

    /// Open the Sample form on a copy of the shared sample. A per-partition sample
    /// splits by a column; partition columns lead the choices, then the columns a
    /// partition is usually made of (text, integers, dates), never floats.
    fn open_sample_form(&mut self) {
        self.open_sample_form_as(false);
    }

    /// A tool with nothing to show yet: the Sample form is its pane, as it stands.
    /// Where the cursor goes is the caller's: into the form when the tool is picked,
    /// back to the tool list when Esc leaves it.
    fn open_first_run_form(&mut self) {
        self.open_sample_form_as(true);
        self.sync_sample_form_focus();
    }

    /// The scope field shows its cursor only while the form has the cursor.
    fn sync_sample_form_focus(&mut self) {
        let focused = self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main;
        if let Some(form) = self.analysis_modal.sample_form.as_mut() {
            let has_cursor = focused || !form.inline;
            form.sync_focus(has_cursor);
        }
    }

    /// Run the tool on screen with the Sample form's sample, or say on the form why
    /// its scope does not parse.
    fn run_sample_form(&mut self) -> Option<AppEvent> {
        let form = self.analysis_modal.sample_form.as_mut()?;
        match form.finish() {
            Ok(sample) => {
                self.analysis_modal.sample_form = None;
                self.apply_sample(sample)
            }
            Err(error) => {
                form.error = Some(error);
                None
            }
        }
    }

    fn open_sample_form_as(&mut self, inline: bool) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let mut partition_columns = state.partition_columns.clone().unwrap_or_default();
        let mut partition_values = Vec::new();
        // A directory whose files agree opens as one scan and names no partition
        // columns; its directory names still do. One branch of the tree is walked for
        // the columns and one listing read for the first column's values: local,
        // and small next to opening the dataset.
        if let Some(dir) = self.path.as_ref().filter(|path| path.is_dir()) {
            if partition_columns.is_empty() {
                partition_columns = DataTableState::discover_hive_partition_columns(dir)
                    .into_iter()
                    .filter(|column| state.schema.get(column).is_some())
                    .collect();
            }
            if let Some(first) = partition_columns.first() {
                let prefix = format!("{first}=");
                let mut values: Vec<String> = std::fs::read_dir(dir)
                    .into_iter()
                    .flatten()
                    .flatten()
                    .filter_map(|entry| {
                        let name = entry.file_name().to_string_lossy().to_string();
                        name.strip_prefix(&prefix).map(str::to_string)
                    })
                    .collect();
                values.sort();
                if !values.is_empty() {
                    partition_values.push((first.clone(), values));
                }
            }
        }
        // An equal-per-value sample splits by a column: partition columns first, then
        // text, the usual stuff of a group (a ticker, a region), then dates and
        // integers. Never floats.
        let mut value_columns = partition_columns.clone();
        for kind in 0..3 {
            for (name, dtype) in state.schema.iter() {
                let rank = match dtype {
                    DataType::String | DataType::Categorical(..) | DataType::Boolean => 0,
                    DataType::Date => 1,
                    dtype if dtype.is_integer() => 2,
                    _ => continue,
                };
                if rank == kind && !value_columns.iter().any(|column| column == name.as_str()) {
                    value_columns.push(name.to_string());
                }
            }
        }
        let context = sample_modal::SampleContext {
            view_rows: state.num_rows_if_valid(),
            filtered: state.changes_rows(),
            files: state.quality_source_file_names().to_vec(),
            partition_columns,
            partition_values,
            time_columns: state.quality_temporal_columns(&data_quality::QualityScope::WholeSource),
            value_columns,
        };
        let mut form =
            sample_modal::SampleForm::new(&self.analysis_modal.sample, context, &self.theme);
        form.inline = inline;
        self.analysis_modal.sample_form = Some(form);
        self.sync_sample_form_focus();
    }

    fn sample_form_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let form = self.analysis_modal.sample_form.as_mut()?;
        let typing = form.field.is_text();
        let on_files = form.field == sample_modal::SampleField::Files;
        let file_count = form.context.files.len();
        match event.code {
            // In a tool's empty pane the form stays, as it was: Esc discards the
            // edit and hands the cursor back to the tool list.
            KeyCode::Esc if form.inline => {
                self.analysis_modal.focus = analysis_modal::AnalysisFocus::Sidebar;
                self.open_first_run_form();
            }
            KeyCode::Esc => self.analysis_modal.sample_form = None,
            KeyCode::Enter => return self.run_sample_form(),
            KeyCode::Down | KeyCode::Tab => form.move_field(true),
            KeyCode::Up | KeyCode::BackTab => form.move_field(false),
            KeyCode::Char('j') if !typing => form.move_field(true),
            KeyCode::Char('k') if !typing => form.move_field(false),
            KeyCode::Left | KeyCode::Char('h') if !typing => form.adjust(false),
            KeyCode::Right | KeyCode::Char('l') if !typing => form.adjust(true),
            KeyCode::PageDown if on_files => {
                form.file_offset = (form.file_offset + crate::widgets::sample_form::FILES_SHOWN)
                    .min(file_count.saturating_sub(1));
            }
            KeyCode::PageUp if on_files => {
                form.file_offset = form
                    .file_offset
                    .saturating_sub(crate::widgets::sample_form::FILES_SHOWN);
            }
            _ if typing => {
                if let Some(input) = form.input_mut(form.field) {
                    let _ = input.handle_key(event, None);
                }
                form.error = None;
            }
            _ => {}
        }
        None
    }

    /// Read the shared sample, as the tool on screen reads it, to show as a table.
    ///
    /// Data Quality's last sample is kept and cut when it is these rows; any other is
    /// drawn again from its seed, which makes it the same rows the tool measured.
    fn read_sample_view(&mut self) -> Option<AppEvent> {
        let sample = self.analysis_modal.sample.clone();
        self.read_sample_rows(sample, None)
    }

    /// Read `sample` off the UI thread and show its rows: all of them, or only a
    /// finding's, under the finding's label. The sample is drawn again from its seed,
    /// so these are the rows the tool measured.
    fn read_sample_rows(
        &mut self,
        sample: sampling::Sample,
        evidence: Option<(polars::prelude::Expr, String)>,
    ) -> Option<AppEvent> {
        let state = self.data_table_state.as_ref()?;
        let (source, known_total) = Self::sample_source_for(state, &sample.scope);
        let streaming = self.app_config.performance.polars_streaming;
        // The rows Data Quality just measured, when they are the rows asked for: cut
        // from memory rather than drawn again from the files.
        let kept = self.kept_quality_sample(&sample).map(|kept| {
            let columns: Vec<_> = state
                .schema
                .iter_names()
                .filter(|name| kept.df().column(name.as_str()).is_ok())
                .map(|name| polars::prelude::col(name.clone()))
                .collect();
            (kept, columns)
        });
        self.reading_sample = true;
        self.analysis_modal.computing = Some(AnalysisProgress::new(if evidence.is_some() {
            "Reading the matching sampled rows"
        } else {
            "Reading the sample"
        }));
        self.busy = true;
        self.spawn_bg("Reading the sample...", move |task_gen, tx| {
            let rows = match kept {
                Some((kept, columns)) => polars::prelude::IntoLazy::lazy(kept.df().clone())
                    .select(columns)
                    .collect()
                    .map_err(color_eyre::eyre::Report::from)
                    .map(|df| kept.analysis_rows(df)),
                None => source
                    .cut(&sample.scope)
                    .and_then(|lf| sampling::read(&lf, &sample, known_total, streaming)),
            };
            let read = rows.and_then(|rows| {
                let label = format!(
                    "Sample {} {}",
                    crate::glyphs::get().middot,
                    sample.outcome(
                        rows.total_rows,
                        rows.sample_size,
                        rows.per_value.as_ref().map(|per_value| per_value.kept),
                    )
                );
                match evidence {
                    Some((predicate, label)) => {
                        let df = polars::prelude::IntoLazy::lazy(rows.df)
                            .filter(predicate)
                            .collect()?;
                        Ok((df, label))
                    }
                    None => Ok((rows.df, label)),
                }
            });
            let _ = tx.send(match read {
                Ok((df, label)) => AppEvent::BackgroundSampleReady {
                    generation: task_gen,
                    label,
                    df,
                },
                Err(error) => AppEvent::BackgroundError {
                    generation: task_gen,
                    message: format!("{error}"),
                },
            });
        });
        None
    }

    /// Put the sample's rows in the table viewer in place of the table, as Data
    /// Quality's drill-in does; Esc brings the table and Analysis back.
    fn show_sample_view(&mut self, df: polars::prelude::DataFrame, label: String) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let view = match state.sample_view(df) {
            Ok(view) => view,
            Err(error) => {
                self.error_modal
                    .show(format!("Cannot show the sample: {error}"));
                return;
            }
        };
        if let Some(original) = self.data_table_state.replace(view) {
            self.quality_evidence_return = Some(Box::new(original));
            self.quality_evidence_label = Some(label);
            self.analysis_modal.active = false;
            self.collect_inflight = None;
            self.spawn_async_collect("Loading the sample...");
        }
    }

    /// Mirror the shared sample into the Data Quality plan, which carries it into the
    /// engine and into the session cache's key. Metadata-only stays metadata-only.
    fn sync_quality_plan(&mut self) {
        let sample = self.analysis_modal.sample.clone();
        let plan = &mut self.analysis_modal.data_quality_plan;
        if plan.scope != sample.scope {
            plan.baseline_segment = None;
        }
        plan.scope = sample.scope;
        plan.sample_seed = sample.seed;
        plan.dataset_rows = sample.rows;
        if plan.compute != data_quality::QualityCompute::Metadata {
            plan.compute = if sample.method == sampling::SampleMethod::EveryRow {
                data_quality::QualityCompute::Full
            } else {
                data_quality::QualityCompute::Sample
            };
        }
        // Choosing equal rows per value of a column is choosing to look at that column's
        // values side by side, and the grain is what does that. Taken only when the
        // choice is new and the grain has not been set, so a grain chosen afterwards
        // stays chosen.
        if let sampling::SampleMethod::PerPartition { column } = &sample.method
            && plan.method != sample.method
            && plan.grain == data_quality::QualityGrain::Dataset
        {
            plan.grain = data_quality::QualityGrain::Partition(column.clone());
            plan.baseline_segment = None;
        }
        plan.method = sample.method;
    }

    /// Adopt a new shared sample: every tool's results were of the old one, so all of
    /// them go, and the tool on screen runs again.
    fn apply_sample(&mut self, sample: sampling::Sample) -> Option<AppEvent> {
        // A first run on the sample as it stands takes nothing from the other tools.
        if sample != self.analysis_modal.sample {
            self.analysis_modal.describe_results = None;
            self.analysis_modal.distribution_results = None;
            self.analysis_modal.correlation_results = None;
            self.analysis_modal.data_quality_results = None;
            self.analysis_modal.data_quality_last_plan = None;
            self.analysis_modal.data_quality_from_cache = false;
        }
        self.analysis_modal.sample = sample;
        self.analysis_modal.sample_dataset = Some(self.dataset_generation);
        self.analysis_modal.sample_run_for = Some(self.dataset_generation);
        self.sync_quality_plan();
        if self.analysis_modal.selected_tool == Some(analysis_modal::AnalysisTool::DataQuality) {
            return self.run_quality_plan();
        }
        self.start_analysis_run()
    }

    /// Run the Data Quality plan as the Plan page's Enter does: from the session cache
    /// when it holds this plan, after confirmation when the plan needs one.
    fn run_quality_plan(&mut self) -> Option<AppEvent> {
        use data_quality::QualityPage;
        self.sync_quality_plan();
        if self.restore_cached_quality() {
            return None;
        }
        self.analysis_modal.set_quality_page(QualityPage::Plan);
        if self
            .analysis_modal
            .data_quality_plan
            .requires_confirmation()
        {
            // The prompt is answered with Enter, which only the main pane hears.
            self.analysis_modal.data_quality_confirm_run = true;
            self.analysis_modal.focus = analysis_modal::AnalysisFocus::Main;
            return None;
        }
        self.analysis_modal.data_quality_results = None;
        self.analysis_modal.data_quality_from_cache = false;
        self.analysis_modal.computing = Some(AnalysisProgress::new("Profiling data quality"));
        self.busy = true;
        Some(AppEvent::AnalysisDataQualityCompute)
    }

    /// Run the selected tool again from scratch, as `r` and `a` do.
    fn start_analysis_run(&mut self) -> Option<AppEvent> {
        let (phase, event) = match self.analysis_modal.selected_tool? {
            analysis_modal::AnalysisTool::Describe => {
                self.analysis_modal.describe_results = None;
                self.analysis_computation = Some(AnalysisComputationState {
                    df: None,
                    schema: None,
                    partial_stats: Vec::new(),
                    current: 0,
                    total: 0,
                    total_rows: 0,
                    sample_seed: self.analysis_modal.sample.seed,
                    sample_size: None,
                });
                ("Describing data", AppEvent::AnalysisChunk)
            }
            analysis_modal::AnalysisTool::DistributionAnalysis => {
                self.analysis_modal.distribution_results = None;
                (
                    "Analyzing distributions",
                    AppEvent::AnalysisDistributionCompute,
                )
            }
            analysis_modal::AnalysisTool::CorrelationMatrix => {
                self.analysis_modal.correlation_results = None;
                (
                    "Computing correlations",
                    AppEvent::AnalysisCorrelationCompute,
                )
            }
            analysis_modal::AnalysisTool::DataQuality => return None,
        };
        self.analysis_modal.computing = Some(AnalysisProgress::new(phase));
        self.busy = true;
        Some(event)
    }

    /// Stop waiting for the analysis in flight.
    ///
    /// Polars cannot stop a query partway, so the worker runs to the end and its answer
    /// is dropped: the bump makes it stale, and its lease no longer holds the table up.
    /// The tool is put back unchosen, so its view does not sit on a spinner for a run
    /// that is not coming; Enter on it runs it again.
    fn cancel_analysis(&mut self) {
        self.task_generation = self.task_generation.wrapping_add(1);
        // Keys typed while it ran were typed at the run, which is gone: an impatient
        // second Enter replayed now would start it again behind the Esc.
        self.screen_generation = self.screen_generation.wrapping_add(1);
        self.analysis_modal.computing = None;
        self.analysis_computation = None;
        self.busy = false;
        self.status_message = None;
        // Reading the sample to look at changed nothing on screen; the tool stays.
        if std::mem::take(&mut self.reading_sample) {
            self.flash_note("Sample view cancelled".to_string());
            return;
        }
        if self.analysis_modal.selected_tool == Some(analysis_modal::AnalysisTool::DataQuality) {
            self.analysis_modal.data_quality_results = None;
        } else {
            self.analysis_modal.selected_tool = None;
            self.analysis_modal.focus = analysis_modal::AnalysisFocus::Sidebar;
        }
        self.flash_note("Analysis cancelled".to_string());
    }

    /// Drill into the group on row `group_index` of the table, whose values are `row`,
    /// and fetch its rows off the UI thread. A drill that fails says why on the control
    /// bar and leaves the grouped view as it was.
    fn drill_into(&mut self, group_index: usize, row: &DataFrame) {
        let Some(state) = self.data_table_state.as_mut() else {
            return;
        };
        state.defer_collect = true;
        let drilled = state.drill_down_with_row(group_index, row);
        state.defer_collect = false;
        match drilled {
            Ok(()) => {
                self.sync_sort_filter_modal();
                self.spawn_async_collect(Self::LOADING_BUFFER);
            }
            Err(e) => self.flash_note(format!(
                "Could not drill in: {}",
                crate::error_display::user_message_from_report(&e, None)
            )),
        }
    }

    /// Show a completion flash on the control bar.
    fn flash_note(&mut self, message: String) {
        self.flash = Some(Flash::new(message));
    }

    /// The completion flash on the control bar, if one is showing.
    pub fn flash_message(&self) -> Option<&str> {
        self.flash.as_ref().map(|f| f.message.as_str())
    }

    /// Drop an expired flash. Returns true when the frame must redraw.
    pub fn tick_flash(&mut self) -> bool {
        if self.flash.as_ref().is_some_and(Flash::expired) {
            self.flash = None;
            return true;
        }
        false
    }

    /// See the `input_dropped` field.
    pub fn set_input_dropped(&mut self, dropped: bool) {
        self.input_dropped = dropped;
    }

    /// The escapes that act at once while busy and jump ahead of anything queued: Ctrl-Q
    /// (and Ctrl-C outside a text field) quit, Ctrl-O goes home, so a slow load never
    /// traps the user; a confirmation modal keeps its keys so it can be answered; and the
    /// home screen is never busy on its own account (only work left running behind it sets
    /// `busy`), so it keeps every key.
    pub fn hard_escape_while_busy(&self, key: &KeyEvent) -> bool {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        let quit = ctrl
            && (key.code == KeyCode::Char('q')
                || (key.code == KeyCode::Char('c') && !self.text_field_focused()));
        let home = ctrl && key.code == KeyCode::Char('o');
        let cancel_analysis = self.analysis_modal.active
            && self.analysis_modal.computing.is_some()
            && key.code == KeyCode::Esc;
        let leave_quality_evidence = self.quality_evidence_return.is_some()
            && self.input_mode == InputMode::Normal
            && key.code == KeyCode::Esc;
        quit || home
            || cancel_analysis
            || leave_quality_evidence
            || self.confirmation_modal.active
            || self.input_mode == InputMode::Home
    }

    /// Whether a key may act while the app is busy. `App::handle` gates on this; the main
    /// loop applies the extra "nothing queued" condition for the second group.
    ///
    /// The hard escapes always qualify. Beyond them, in the plain Normal-mode table view
    /// (no text field, no modal), the harmless view keys act — quit, column scroll and
    /// help — because the first key held in that view cannot be part of a typed
    /// `/query`. Harmless means reads nothing: column scroll re-slices the buffer it
    /// already holds through `rescroll_columns`, never `collect`, which counts the rows
    /// when the count has not landed. Admitting a key that can count would put a
    /// metadata read per object of a cloud hive on this very thread —
    /// `a_key_that_acts_while_busy_reads_nothing` holds the line. Everything else,
    /// letters included, is type-ahead and waits; a bare Enter or Esc there confirms
    /// nothing and is dropped by the caller. Nothing is classified by keycode alone:
    /// the `h` in a typed `/hello` never scrolls.
    pub fn key_acts_while_busy(&self, key: &KeyEvent) -> bool {
        if self.hard_escape_while_busy(key) {
            return true;
        }
        self.in_normal_table_view()
            && matches!(
                key.code,
                KeyCode::Char('q')
                    | KeyCode::Char('Q')
                    | KeyCode::Left
                    | KeyCode::Right
                    | KeyCode::Char('h')
                    | KeyCode::Char('l')
                    | KeyCode::F(1)
                    | KeyCode::Char('?')
            )
    }

    /// The plain table view: Normal mode with no help overlay, modal, or in-view modal
    /// (template, analysis) drawn over it.
    pub fn in_normal_table_view(&self) -> bool {
        self.input_mode == InputMode::Normal
            && !self.show_help
            && !self.template_modal.active
            && !self.analysis_modal.active
            && !self.error_modal.active
            && !self.confirmation_modal.active
    }

    /// Whether a text field currently owns typed characters, so Ctrl-C copies rather than
    /// quits. The home filter is deliberately excluded: Ctrl-C quits from the home screen.
    pub fn text_field_focused(&self) -> bool {
        match self.input_mode {
            InputMode::Editing => true,
            InputMode::Export => matches!(
                self.export_modal.focus,
                ExportFocus::PathInput | ExportFocus::CsvDelimiter
            ),
            // The Picker narrows by typing, so it types.
            InputMode::Copy => self.copy_modal.picker.is_some(),
            InputMode::SortFilter => {
                self.sort_filter_modal.focus == SortFilterFocus::Body
                    && match self.sort_filter_modal.active_tab {
                        // The whole inline editor types: pickers narrow, the value edits.
                        SortFilterTab::Filter => self.sort_filter_modal.filter.editor.is_some(),
                        SortFilterTab::Sort => {
                            self.sort_filter_modal.sort.focus == SortFocus::Filter
                        }
                    }
            }
            InputMode::PivotMelt => {
                // The Picker narrows by typing, so it types too.
                self.pivot_melt_modal.picker.is_some()
                    || self
                        .pivot_melt_modal
                        .is_text_row(self.pivot_melt_modal.focus)
            }
            InputMode::Chart => {
                if self.chart_export_modal.active {
                    matches!(
                        self.chart_export_modal.focus,
                        ChartExportFocus::PathInput
                            | ChartExportFocus::TitleInput
                            | ChartExportFocus::WidthInput
                            | ChartExportFocus::HeightInput
                    )
                } else {
                    // The open column Picker narrows by typing, so it types.
                    self.chart_modal.picker.is_some()
                }
            }
            InputMode::Normal => {
                self.analysis_modal.sample_scope_typing()
                    || (self.template_modal.active
                        && self.template_modal.mode != TemplateModalMode::List
                        && matches!(
                            self.template_modal.form_focus,
                            FormFocus::Name
                                | FormFocus::Description
                                | FormFocus::ExactPath
                                | FormFocus::RelativePath
                                | FormFocus::PathPattern
                                | FormFocus::FilenamePattern
                        ))
            }
            InputMode::Home | InputMode::Info => false,
        }
    }

    pub fn send_event(&mut self, event: AppEvent) -> Result<()> {
        self.events.send(event)?;
        Ok(())
    }

    /// Whether the dataset on screen is one that opened before its footers were read
    /// and is still waiting for them.
    ///
    /// Not the same question as whether a pass is running. The counter is shared with
    /// every open, and abandoning one does not stop it: without this, giving up on a
    /// large local directory and going back to the dataset you had would leave that
    /// dataset's control bar counting footers belonging to the directory you left.
    /// Whether the row count on the control bar is on its way, so a spinner stands in
    /// for it. Asked by the bar, and by the run loop, which turns the spinner: the
    /// two disagreed while a dataset read its own footers, and the spinner sat still.
    pub fn row_count_pending(&self) -> bool {
        // A load in flight counts as pending: the number `data_table_state` still holds
        // belongs to the dataset being replaced, and printing it beside the incoming
        // file's name would read as the new one's.
        // A dataset still reading its own footers counts too: it declines the ordinary
        // count because that pass is bringing one, so nothing is "in flight" — and the
        // number it holds meanwhile is only as far as the buffer reaches. Printed
        // plainly, a prefix of six thousand files reads `Rows: 70`.
        self.len_count_inflight.is_some()
            || self.awaiting_dataset
            // A re-read owed to a dataset whose footers could not be read is a count
            // that is coming: the collect it is waiting to run is what starts one. The
            // dataset has already stopped saying it counts itself later (it gave up on
            // the pass the moment that pass failed), so without this the bar falls
            // through to printing the number it happens to hold — which is only as far
            // as the buffer reached. A prefix of six thousand files reads `Rows: 70`,
            // plainly, for as long as the work in front of the errand takes.
            || self.reread_owed.is_some()
            || self
                .data_table_state
                .as_ref()
                .is_some_and(|state| state.counts_itself_later())
    }

    /// Whether a spinner is on screen, so the run loop turns it and redraws.
    pub fn something_is_spinning(&self) -> bool {
        self.busy
            || self.row_count_pending()
            || self.chart_preparing()
            || (self.input_mode == InputMode::Home
                && (self.home.awaiting_listing().is_some()
                    || self.home.sections_waiting()
                    || !self.home.peeking.is_empty()))
    }

    fn dataset_is_still_reading_its_footers(&self) -> bool {
        self.data_table_state
            .as_ref()
            .is_some_and(|state| state.footers_pending().is_some())
    }

    /// Take the numbers the whole frame will be drawn from.
    ///
    /// Only one so far: the footer count. It is read here rather than where it is
    /// shown because two parts of the screen show it, they are painted at different
    /// moments, and a background thread is moving it between them.
    fn begin_frame(&mut self) {
        self.footers_this_frame = self.footer_progress.reading();
    }

    /// What the load is doing, for whichever part of the screen is saying so.
    ///
    /// The footer count stands in for the phase while a pass is running: it says the
    /// same thing and says how far along it is. Both callers read it from
    /// [`Self::footers_this_frame`], one number taken once a frame, so they cannot say
    /// two different things about one wait.
    pub(crate) fn loading_phase<'a>(&self, phase: &'a str) -> std::borrow::Cow<'a, str> {
        match self.footers_this_frame {
            Some((read, total)) => std::borrow::Cow::Owned(format!(
                "Reading footers: {} of {}",
                crate::numfmt::group_chrome(read),
                crate::numfmt::group_chrome(total)
            )),
            None => std::borrow::Cow::Borrowed(phase),
        }
    }

    /// Set loading state and phase so the progress dialog is visible. Used by run() to show
    /// loading UI immediately when launching from LazyFrame (e.g. Python) before sending the open event.
    pub fn set_loading_phase(&mut self, phase: impl Into<String>, progress_percent: u16) {
        self.busy = true;
        // A frame is drawn between the keypress that starts a load and the `Open` that
        // carries it out, so the handover has to happen here too or that frame still
        // shows the outgoing dataset.
        self.awaiting_dataset = true;
        self.loading_state = LoadingState::Loading {
            file_path: None,
            file_size: 0,
            current_phase: phase.into(),
            progress_percent,
        };
    }

    /// Apply a successfully loaded DataTableState to the app. Shared by all schema load paths.
    /// Read the rows on screen again, now that the frame they were read through has
    /// been replaced.
    ///
    /// Not through `DoLoadBuffer`: that is a step of the open's chain and is ignored
    /// unless a load is in progress, and this happens long after the load has finished
    /// — and after a glance at the home screen, never again. The join has already
    /// dropped the buffer, so nothing dropping this leaves the table with no rows to
    /// show at the moment it was to show more of them.
    fn reread_after_the_footers_joined(&mut self) {
        // Any re-read satisfies one that was owed: this is the collect the errand was
        // waiting to run, whoever asked for it.
        self.reread_owed = None;
        // End was pressed while the footers were still coming, and they are what the
        // end was waiting on. Taken either way: a flag left from a dataset that is gone
        // is not this one's to act on. The jump reads the page it lands on, so reading
        // the page here first would be one fetched to be thrown away.
        if self.end_when_the_footers_land.take() == Some(self.dataset_generation) {
            self.status_message = None;
            if let Some(next) = self.jump_key(AppEvent::DoScrollEnd) {
                // The jump reads the page it lands on, so reading this one first would
                // be a page fetched to be thrown away.
                let _ = self.events.send(next);
                return;
            }
            // Unless it asked for no read: the view was already at the end, or the pass
            // brought no count and the jump is waiting on the ordinary one. The join has
            // dropped the buffer either way, so falling through is the difference
            // between a table and an empty one.
        }
        self.spawn_async_collect(Self::LOADING_BUFFER);
    }

    /// Run a buffer collect that was asked for while other work was waiting on the
    /// generation.
    ///
    /// The same shape as `reread_when_the_work_allows` below, and for the same reason:
    /// the collect bumps `task_generation`, so it waits its turn and is tried again
    /// after every event.
    fn collect_when_the_work_allows(&mut self) {
        let Some((generation, _)) = self.collect_owed.as_ref() else {
            return;
        };
        if *generation != self.dataset_generation {
            // The dataset it was owed to is gone, and so is the view it was filling.
            // Only the errand is put down: `busy` and the status line belong to whatever
            // replaced the dataset, and are not this errand's to clear.
            self.collect_owed = None;
            return;
        }
        if self.work_a_bump_would_strand() {
            return;
        }
        let Some((_, status)) = self.collect_owed.take() else {
            return;
        };
        if !self.spawn_async_collect(&status) {
            self.busy = false;
            self.status_message = None;
            // The collect that was owed may have been the last step of an open, and
            // `DoLoadBuffer` takes the loading screen down itself when there turns out
            // to be nothing to collect. Deferred, that branch is not the one that runs,
            // and the screen would read "Loading buffer... 70%" with the app idle for
            // the rest of the session. Only a load's own state: an export owns
            // `loading_state` too, and it is still going.
            if matches!(self.loading_state, LoadingState::Loading { .. }) {
                self.loading_state = LoadingState::Idle;
            }
        }
    }

    /// Run the re-read a failed footer pass owes the dataset, once it can be run
    /// without throwing another answer away.
    ///
    /// The failure branch of `BackgroundFootersJoined` used to re-read on the spot,
    /// which bumped `task_generation` with no check at all — the one path into the
    /// collect that never asked `work_the_join_would_cancel`. An export in its collect
    /// phase then never wrote its file and said nothing about it. So the errand waits
    /// its turn, the way held columns already do.
    fn reread_when_the_work_allows(&mut self) {
        let Some(generation) = self.reread_owed else {
            return;
        };
        if generation != self.dataset_generation {
            // The dataset it was owed to is gone; so is the errand.
            self.reread_owed = None;
            return;
        }
        if self.work_the_join_would_cancel() {
            return;
        }
        self.reread_after_the_footers_joined();
    }

    /// Retire an End that was waiting on a count which can no longer answer it.
    ///
    /// Only the flag and the message it put up: the jump itself is not re-issued. See
    /// the caller in `BackgroundLenReady` for why asking again is the wrong repair.
    fn retire_the_end_that_was_waiting(&mut self) {
        self.end_after_count = None;
        self.take_down_the_counting_status();
    }

    /// Take down "Counting rows to find the end...", and only that.
    ///
    /// Clearing the status outright would wipe whatever else is using the line — a
    /// load's phase, an export's progress — on behalf of a key pressed somewhere else.
    fn take_down_the_counting_status(&mut self) {
        if self.status_message.as_deref() == Some(Self::COUNTING_FOR_END) {
            self.status_message = None;
        }
    }

    /// What the status line says while an End is waiting on a row count. Named so the
    /// paths that retire such an End can take the message back down without reaching
    /// for a literal, and without clearing a message that belongs to something else.
    const COUNTING_FOR_END: &'static str = "Counting rows to find the end...";

    /// What the control bar says while a path is being looked at. Named so the answer can
    /// take down its own line without clearing one that belongs to something else.
    const LOOKING: &'static str = "Looking...";

    /// The wait while a directory named on the command line is looked at: which files it
    /// holds, and whether they are one table. Seconds, for a directory of large Parquet.
    pub const LOOKING_AT_A_DIRECTORY: &'static str = "Looking at the directory";

    /// The wait while the rows for the view are fetched.
    pub const LOADING_BUFFER: &'static str = "Loading buffer...";

    /// The wait while a grouped row the buffer does not hold is read to drill into.
    const READING_GROUP: &'static str = "Reading the group...";

    /// How long a fetch goes unmentioned. A local page lands well inside it, and the key
    /// chips staying put is the difference between paging and a bar that blinks a
    /// sentence on every screen.
    const A_FETCH_WORTH_SAYING: std::time::Duration = std::time::Duration::from_millis(300);

    /// Grow the buffer before the view reaches its end, rather than once it has.
    ///
    /// Nothing waits on it: no `busy`, no message, and keys go on paging through the
    /// rows on hand. One at a time — a scroll that outruns it either waits on it, when it
    /// is bringing the rows asked for, or supersedes it by the generation, as any newer
    /// collect does. Never when a bump would strand other work.
    fn load_ahead(&mut self) {
        if self.busy
            || self.collect_owed.is_some()
            || self
                .collect_inflight
                .is_some_and(|inflight| inflight.generation == self.task_generation)
        {
            return;
        }
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        // Asked of each position once. Planning can give back the buffer on hand — a
        // row group too large to add under the caps — and asking again every frame
        // would plan it again every frame.
        let position = state.buffer_position();
        if !state.wants_to_load_ahead() || self.loaded_ahead_from == Some(position) {
            return;
        }
        self.loaded_ahead_from = Some(position);
        if self.work_a_bump_would_strand() {
            return;
        }
        self.spawn_collect(None);
    }

    /// Whether the bar is still keeping quiet about a fetch for the view.
    fn fetch_too_young_to_mention(&self) -> bool {
        self.status_message.as_deref() == Some(Self::LOADING_BUFFER)
            && self.collect_inflight.is_some_and(|inflight| {
                inflight.generation == self.task_generation
                    && inflight.began.elapsed() < Self::A_FETCH_WORTH_SAYING
            })
    }

    /// Put the path on the loading screen, so a wait says what it is waiting for.
    fn name_what_is_loading(&mut self, path: PathBuf) {
        if let LoadingState::Loading { file_path, .. } = &mut self.loading_state {
            *file_path = Some(path);
        }
    }

    /// Work already running that the re-read after a join would cancel.
    ///
    /// The re-read goes through the ordinary collect, which bumps `task_generation`, so
    /// everything a bump would strand has to be done first — and that is
    /// [`GenerationLease`]'s job now, rather than a list of the kinds of work that
    /// might be running.
    ///
    /// One thing more than a bump, though: a join takes a fresh `len_generation` too. A
    /// chart is prepared against the frame rather than the generation
    /// (`BackgroundChartReady` carries no generation at all), so a bump cannot strand
    /// one but changing the frame under it can.
    fn work_the_join_would_cancel(&self) -> bool {
        self.work_a_bump_would_strand() || self.chart_preparing()
    }

    /// Give the dataset what its footers found, if it can take it now.
    ///
    /// It cannot while the user is looking at a query, a pivot, a melt or a drill-down:
    /// those make their own result the root, and widening the scan underneath one takes
    /// away the columns it is built from. So the columns wait — held, not dropped — and
    /// this is tried again after every event, which is the cheapest way to catch the
    /// moment the view comes back to the data.
    ///
    /// Returns whether the dataset took them, so the caller can re-read the rows on
    /// screen through the wider frame.
    fn join_held_footers(&mut self) -> bool {
        let Some((generation, _)) = self.footers_held.as_ref() else {
            return false;
        };
        if *generation != self.dataset_generation {
            // The dataset they belong to is gone; so are they.
            self.footers_held = None;
            return false;
        }
        if self.data_table_state.is_none() || self.work_the_join_would_cancel() {
            return false;
        }
        let Some((generation, found)) = self.footers_held.take() else {
            return false;
        };
        let state = self
            .data_table_state
            .as_mut()
            .expect("checked just above, and nothing since takes it");
        // Whether this is the moment is the dataset's call, not this one's: it is the
        // frame on screen that knows whether it still grows from the scan.
        match state.join_dataset_schema(found) {
            Ok(()) => true,
            Err(found) => {
                self.footers_held = Some((generation, *found));
                false
            }
        }
    }

    /// Start the pass that reads the rest of a staged open's footers.
    ///
    /// Not through `spawn_bg`, which marks the app busy: the whole point of opening
    /// before every footer is read is that the dataset works while they are read. The
    /// generation is the dataset's rather than the task's, because a collect bumps the
    /// task's and this pass outlives several of them.
    fn start_pending_footers(&mut self) {
        let Some(join) = self
            .data_table_state
            .as_ref()
            .and_then(|state| state.footers_pending())
        else {
            return;
        };
        let generation = self.dataset_generation;
        let slot = self.pending_footers_result.clone();
        let tx = self.events.clone();
        let progress = self.footer_progress.clone();
        self.runtime.spawn_blocking(move || {
            // Reported either way. A pass that could not read them has to say so, or
            // the dataset waits for it for the rest of the session — and a waiting
            // dataset is one that will not count itself, because the count was what
            // the pass was bringing back.
            let found = join(&progress);
            if !Self::record_footers(&slot, generation, found) {
                return;
            }
            let _ = tx.send(AppEvent::BackgroundFootersJoined { generation });
        });
    }

    /// Put what a pass found in the slot, unless a later dataset's pass has answered
    /// first. Returns whether it went in, so a pass that lost does not also announce
    /// itself.
    ///
    /// Two passes can be in flight at once — opening a second large prefix does not
    /// stop the first one reading — and they finish in whatever order the network
    /// gives. Without this the slower, older one overwrites the newer entry, and the
    /// generation the event carries then disagrees with the generation in the slot,
    /// so both are discarded and the dataset on screen never gets its columns.
    fn record_footers(
        slot: &std::sync::Mutex<FootersReported>,
        generation: u64,
        found: Option<crate::widgets::datatable::FootersFound>,
    ) -> bool {
        let mut slot = slot.lock().unwrap_or_else(|e| e.into_inner());
        if slot.as_ref().is_some_and(|(held, _)| *held > generation) {
            return false;
        }
        *slot = Some((generation, found));
        true
    }

    fn apply_schema_ready(
        &mut self,
        state: DataTableState,
        path: Option<PathBuf>,
        options: &OpenOptions,
        debug_label: Option<String>,
    ) {
        // Installing a dataset is the point of no return for abandonment, so every
        // caller has to have checked. A new one that forgets swaps a dataset in
        // underneath the home screen.
        debug_assert!(
            self.load_active,
            "apply_schema_ready called for an abandoned load"
        );
        // A key pressed at the dataset being replaced belongs to it, not to this one.
        self.end_when_the_footers_land = None;
        // Its companion, for the same reason. This one keys itself to a
        // `len_generation`, which says nothing about which dataset it belonged to, so
        // without clearing it here an End pressed on the directory the user walked away
        // from is still live against the one they opened next.
        self.end_after_count = None;
        // One per dataset that reaches the screen, rather than one per open started:
        // an open that fails leaves the last dataset up, and the pass still reading its
        // footers has to be able to finish into it.
        self.dataset_generation = self.dataset_generation.wrapping_add(1);
        self.quality_cache.clear();
        self.quality_sample = None;
        self.quality_evidence_return = None;
        self.quality_evidence_label = None;
        // Whatever chart state survived belongs to the dataset being replaced.
        self.reset_chart_state();
        self.debug.schema_load = debug_label;
        self.awaiting_dataset = false;
        // Home is now in the stack, so q pops back to it; never unset, since a
        // reread from the table (H) is not a new place.
        if self.load_from_home {
            self.opened_from_home = true;
        }
        self.load_from_home = false;
        if let Some(paths) = self.opening.take() {
            self.opened = Some((paths, options.clone()));
        }
        if let Some(path) = self.recent_on_install.take() {
            // Off the opening path. Recording a recent is a convenience that nothing
            // waits on, and it takes a lock several instances may be contending for --
            // opening a dataset must not queue behind another instance's bookkeeping.
            let cache = self.cache.clone();
            std::thread::spawn(move || cache.push_recent(&path));
        }
        self.collect_inflight = None;
        self.parquet_metadata_cache = None;
        self.export_df = None;
        self.data_table_state = Some(state);
        self.path = path.clone();
        if let Some(ref p) = path {
            self.original_file_format = Self::export_format_for(p, options);
            // A comma unless the user named a separator. A `.tsv` exports as CSV, to a
            // `.csv` by default, and a tab there would reopen as one column.
            self.original_file_delimiter = Some(options.separator_or(b','));
        } else {
            self.original_file_format = None;
            self.original_file_delimiter = None;
        }
        // Enable the cheap footer-sum row count for a local Parquet hive directory.
        if options.hive
            && let Some(p) = path.as_ref().filter(|p| p.is_dir())
            && let Some(state) = self.data_table_state.as_mut()
        {
            state.set_parquet_count_dir(p.clone());
        }
        // The dataset is on screen now; whatever it still has to learn about itself is
        // read behind it.
        self.start_pending_footers();
        self.sort_filter_modal = SortFilterModal::new();
        self.pivot_melt_modal = PivotMeltModal::new();
        if let LoadingState::Loading {
            file_path,
            file_size,
            ..
        } = &self.loading_state
        {
            self.loading_state = LoadingState::Loading {
                file_path: file_path.clone(),
                file_size: *file_size,
                current_phase: "Loading buffer".to_string(),
                progress_percent: 70,
            };
        }
        self.status_message = Some(Self::LOADING_BUFFER.to_string());

        // The dataset is installed and its schema known, so this is where a template
        // meets it. `--template` names one and applies to this first open alone;
        // `[templates] auto_apply` dresses every open that has a matching template.
        // A fresh dataset starts with no view applied: the previous file's view
        // must not wear the check mark here, nor count as applied when edited.
        self.active_template_id = None;
        if let Some(name) = self.startup_template.take() {
            match self.template_manager.get_template_by_name(&name).cloned() {
                Some(template) => {
                    if let Err(e) = self.apply_template(&template) {
                        self.error_modal
                            .show(format!("Error applying view \"{name}\": {e}"));
                    }
                }
                None => self.error_modal.show(format!("No view named \"{name}\"")),
            }
        } else if self.app_config.templates.auto_apply
            && let Some(path) = self.path.clone()
            && let Some(template) = self.data_table_state.as_ref().and_then(|state| {
                self.template_manager
                    .get_most_relevant(&path, state.source_schema())
            })
            && let Err(e) = self.apply_template(&template)
        {
            self.error_modal
                .show(format!("Error applying view \"{}\": {e}", template.name));
        }
    }

    /// Ensures file path has an extension when user did not provide one; only adds
    /// compression suffix (e.g. .gz) when compression is selected. If the user
    /// provided a path with an extension (e.g. foo.feather), that extension is kept.
    /// Spawn an async buffer collect if needed. Returns true if a background task was spawned.
    /// Increments task_generation to invalidate any in-flight collect from a prior call.
    ///
    /// When the LazyFrame's row count is unknown (e.g. fresh load, or just after a
    /// filter/sort/pivot/melt that invalidates the cache), the exact `len()` is computed
    /// in the background and applied later via `BackgroundLenReady` — it never gates the
    /// buffer paint. `prepare_async_collect` plans a top-of-data window when the count is
    /// still unknown, so the first screen renders immediately. For large/partitioned/remote
    /// datasets the count can take a long time; it runs silently and concurrently.
    pub fn spawn_async_collect(&mut self, status: &str) -> bool {
        self.spawn_collect(Some(status))
    }

    /// As [`Self::spawn_async_collect`]; with no `status`, a load-ahead that nothing
    /// waits on. See [`InflightCollect::waited_on`].
    fn spawn_collect(&mut self, status: Option<&str>) -> bool {
        let Some(state) = self.data_table_state.as_mut() else {
            return false;
        };

        // The exact row count, when it isn't known and none is already running for
        // this data version. Independent of `task_generation` (a scroll must not
        // restart it) and does not set `busy`. On a local file it runs alongside the
        // buffer collect; on an object store it rides in the collect spawned below,
        // which answers it outright when the read comes back short and otherwise
        // gets the row groups to itself first — unless no collect is spawned, when it
        // runs on its own after all.
        let mut count = None;
        if !state.is_num_rows_valid() && self.len_count_inflight != Some(state.len_generation()) {
            let job = LenCount::for_state(state);
            // Marked as running only once it is going to run. A dataset still reading
            // its own footers declines this count, because that pass is bringing it —
            // and the marker is cleared by a count coming back, so setting it for one
            // that was never started leaves it set for the rest of the session: a
            // spinner where the row count goes, a redraw on its account every frame,
            // and `End` waiting on nothing.
            if state.counts_itself_later() {
                count = None;
            } else {
                self.len_count_inflight = Some(job.len_generation);
                // A fresh attempt for this generation clears any prior failure marker.
                if self.len_count_failed == Some(job.len_generation) {
                    self.len_count_failed = None;
                }
                count = Some(job);
            }
        }
        // A footer count runs now too, remote or not: it reads no data.
        if let Some(job) = count.take_if(|job| !state.is_remote_source() || job.reads_footers()) {
            let tx = self.events.clone();
            self.runtime
                .spawn_blocking(move || job.send(job.run(), &tx));
        }

        // Read before the frame is borrowed: the predicate is over the whole App.
        let a_bump_would_strand = self.work_a_bump_would_strand();

        // Plan and spawn the buffer collect. With the count unknown this is a top-of-data
        // window (`slice(0, N)`) that touches only the first file(s) of a partitioned set.
        let Some(state) = self.data_table_state.as_mut() else {
            return false;
        };
        let covered = self
            .collect_inflight
            .is_some_and(|inflight| inflight.covers(self.task_generation, state));
        // The rows asked for are already on the way in a load-ahead: wait on that one
        // rather than fetch them twice.
        if covered
            && let Some(status) = status
            && let Some(inflight) = self.collect_inflight.as_mut()
            && !inflight.waited_on
        {
            inflight.waited_on = true;
            self.busy = true;
            self.status_message = Some(status.to_string());
        }
        let request = (!covered).then(|| state.prepare_async_collect(None));
        let Some(Some(request)) = request else {
            // Nothing to ride in: the view is covered, or the buffer on hand serves it.
            if let Some(job) = count {
                let tx = self.events.clone();
                self.runtime
                    .spawn_blocking(move || job.send(job.run(), &tx));
            }
            return covered;
        };
        // Everything past here bumps `task_generation`, so everything holding a lease on
        // it has to be done first. The collect the user asked for is queued rather than
        // refused: the throbber that was already turning goes on turning, and it is
        // tried again after every event until the work in front of it finishes.
        //
        // This is the door #238 was about. `BackgroundLenReady` answers a count by
        // jumping to the end, which reaches here with no key pressed and minutes after
        // the one that was — long enough for a dataset to have been opened meanwhile.
        // The bump cancelled that open, and `BackgroundSchemaReady`'s mismatch branch
        // returns without resetting anything, so `awaiting_dataset`, `busy` and
        // `loading_state` stayed set and the file never opened, silently, for the rest
        // of the session.
        if a_bump_would_strand {
            // The count that was going to ride in this collect is put down rather than
            // run on its own. On an object store it answers itself out of the short read
            // the collect comes back with; spawned standalone it is a full remote
            // `len()`, which is the expensive thing the riding exists to avoid. Putting
            // the marker down with it is what lets the retry ask again.
            if count.is_some() {
                self.len_count_inflight = None;
            }
            // A load-ahead is not owed: nobody asked for it.
            let Some(status) = status else {
                return false;
            };
            self.collect_owed = Some((self.dataset_generation, status.to_string()));
            return true;
        }
        self.task_generation = self.task_generation.wrapping_add(1);
        self.collect_inflight = Some(InflightCollect {
            began: std::time::Instant::now(),
            files: state.files_a_page_reads(
                request.buffer_start,
                request.buffer_end.saturating_sub(request.buffer_start),
            ),
            generation: self.task_generation,
            dataset: state.len_generation(),
            start: request.buffer_start,
            end: request.buffer_end,
            waited_on: status.is_some(),
        });
        let collect_slot = self.pending_collect_result.clone();
        self.spawn_bg_replaceable(status, move |task_gen, tx| {
            match crate::statistics::collect_lazy(request.lf, request.polars_streaming) {
                Ok(df) => {
                    let returned = df.height();
                    let mut slot = collect_slot.lock().unwrap_or_else(|e| e.into_inner());
                    // Only write if no newer result is already stored.
                    let dominated = slot.as_ref().is_some_and(|(g, _)| *g > task_gen);
                    if !dominated {
                        *slot = Some((
                            task_gen,
                            crate::widgets::datatable::CollectResult {
                                df,
                                buffer_start: request.buffer_start,
                                buffer_end: request.buffer_end,
                                num_rows: request.num_rows,
                                count_known: request.count_known,
                            },
                        ));
                    }
                    drop(slot);
                    let _ = tx.send(AppEvent::BackgroundCollectReady {
                        generation: task_gen,
                    });
                    if let Some(job) = count {
                        let requested = request.buffer_end - request.buffer_start;
                        let counted = job.after_collect(request.buffer_start, returned, requested);
                        job.send(counted, &tx);
                    }
                }
                Err(e) => {
                    let _ = tx.send(AppEvent::BackgroundCollectFailed {
                        generation: task_gen,
                        message: crate::error_display::user_message_from_polars(&e),
                    });
                    // A pass over a frame that just failed to collect would fail too:
                    // report the count as failed and leave the retry to a later
                    // interaction (see `len_count_failed`).
                    if let Some(job) = count {
                        job.send(Err(()), &tx);
                    }
                }
            }
        });
        true
    }

    /// Spawn a background task. Captures the current generation and event sender
    /// for the closure, sets `busy` and `status_message`. The closure is responsible
    /// for sending a follow-up event (typically `Background*Ready` or `BackgroundError`)
    /// using the captured generation so stale results can be filtered out.
    ///
    /// Does not bump `task_generation`. Callers that need to invalidate prior in-flight
    /// work should bump it explicitly before calling.
    fn spawn_bg<F>(&mut self, status: &str, work: F)
    where
        F: FnOnce(u64, Sender<AppEvent>) + Send + 'static,
    {
        let lease = self.lease_the_generation();
        self.spawn_bg_inner(Some(status), Some(lease), work);
    }

    /// Spawn background work whose answer, thrown away by a bump, is simply asked for
    /// again — the buffer collect, and only that.
    ///
    /// It holds no [`GenerationLease`], because the collect is what a lease makes wait:
    /// leased, the next collect would queue behind the last one and scrolling would go
    /// a page per round trip. Supersession is `InflightCollect::covers`'s job instead.
    ///
    /// This is the one exemption, and `the_collect_is_the_only_unleased_spawn` fails if
    /// a second one appears.
    ///
    /// With no `status` it sets neither `busy` nor a message: a load-ahead.
    fn spawn_bg_replaceable<F>(&mut self, status: Option<&str>, work: F)
    where
        F: FnOnce(u64, Sender<AppEvent>) + Send + 'static,
    {
        self.spawn_bg_inner(status, None, work);
    }

    fn spawn_bg_inner<F>(&mut self, status: Option<&str>, lease: Option<GenerationLease>, work: F)
    where
        F: FnOnce(u64, Sender<AppEvent>) + Send + 'static,
    {
        let task_gen = self.task_generation;
        let tx = self.events.clone();
        if let Some(status) = status {
            self.busy = true;
            self.status_message = Some(status.to_string());
        }
        self.runtime.spawn_blocking(move || {
            // Dropped after `work` returns, and on the way out of a panic too.
            let _lease = lease;
            work(task_gen, tx);
        });
    }

    /// A lease held by nothing, for tests that need work in flight without a thread to
    /// run it on. Exposed for the same reason `task_generation` is.
    #[cfg(test)]
    pub(crate) fn lease_for_tests(&mut self) -> GenerationLease {
        self.lease_the_generation()
    }

    /// Take a lease on the current `task_generation`. See [`GenerationLease`].
    fn lease_the_generation(&mut self) -> GenerationLease {
        *self.leases.entry(self.task_generation).or_default() += 1;
        GenerationLease {
            events: self.events.clone(),
            generation: self.task_generation,
        }
    }

    /// Whether anything is waiting on the current `task_generation`, so that bumping it
    /// would throw away an answer nothing will ask for again.
    fn work_a_bump_would_strand(&self) -> bool {
        // A count, and nothing else. Nothing here names a kind of work, so a new kind is
        // covered by taking a lease rather than by being remembered here — which is the
        // whole of #221. Three things hold one:
        //
        //  - every background spawn, for as long as its worker runs ([`App::spawn_bg`]);
        //  - `EventPump`, for as long as a continuation it has not dispatched is
        //    waiting, which is the gap between two phases of one errand;
        //  - an errand parked on the user, which is the download confirmation.
        //
        // Counted per generation, and only the current one asked about: work leased on an
        // older one was cancelled by the bump that passed it — an analysis the user
        // stopped with Esc, whose worker Polars cannot interrupt — and waiting for it
        // would hold the table up for work nobody wants.
        self.leases
            .get(&self.task_generation)
            .is_some_and(|n| *n > 0)
    }

    /// Run a scroll on `data_table_state` and resolve the busy/spawn cycle.
    /// `scroll` returns true when its movement leaves the buffered window (caller must collect).
    /// We clear `busy` ourselves when no collect is needed or the spawn no-ops, otherwise
    /// the busy flag set by the key handler would gate further input forever.
    /// Home, End and G. A jump may need a fill, so it is deferred behind a frame that
    /// shows the throbber — setting `start_row` alone used to leave the old buffer on
    /// screen, drawn from its first row — unless the view is already there, in which
    /// case only the selection settles and no frame or key is spent.
    fn jump_key(&mut self, jump: AppEvent) -> Option<AppEvent> {
        // The end of a remote dataset is not known until its rows are counted, and a
        // jump to a guess reads every file up to it. Wait for the count instead; keys
        // keep working meanwhile.
        // A dataset still reading its own footers is already getting a count, and its
        // end is known as soon as that lands. Starting one here would read every footer
        // a second time — and the join takes a fresh `len_generation` on its way past,
        // so the count that came back would be answering a question nobody could match
        // it to and the jump would never happen. Wait for the pass instead.
        // `scan_is_the_root`, not `counts_itself_later`: the question here is whether a
        // join is going to land underneath this frame and take a fresh `len_generation`
        // with it, which is what would leave a count answering a question nothing could
        // match it to. A filter and a sort are rebuilt over the joined scan, so they are
        // on this side of it even though they are not pristine.
        if matches!(jump, AppEvent::DoScrollEnd)
            && let Some(state) = self.data_table_state.as_ref()
            && state.footers_pending().is_some()
            && state.scan_is_the_root()
            && !state.is_num_rows_valid()
        {
            self.end_when_the_footers_land = Some(self.dataset_generation);
            self.status_message = Some(Self::COUNTING_FOR_END.to_string());
            return None;
        }
        if matches!(jump, AppEvent::DoScrollEnd)
            && let Some(state) = self.data_table_state.as_ref()
            && state.is_remote_source()
            && !state.is_num_rows_valid()
        {
            let generation = state.len_generation();
            self.end_after_count = Some(generation);
            self.status_message = Some(Self::COUNTING_FOR_END.to_string());
            if self.len_count_inflight != Some(generation) {
                let job = LenCount::for_state(state);
                self.len_count_inflight = Some(generation);
                let tx = self.events.clone();
                self.runtime
                    .spawn_blocking(move || job.send(job.run(), &tx));
            }
            return None;
        }
        let state = self.data_table_state.as_mut()?;
        let (already_there, settle): (bool, fn(&mut DataTableState) -> bool) = match jump {
            AppEvent::DoScrollHome => (state.start_row == 0, DataTableState::scroll_to_start),
            _ => (state.at_end(), DataTableState::scroll_to_end),
        };
        if already_there {
            settle(state);
            return None;
        }
        self.busy = true;
        Some(jump)
    }

    fn handle_scroll<F>(&mut self, scroll: F) -> Option<AppEvent>
    where
        F: FnOnce(&mut crate::widgets::datatable::DataTableState) -> bool,
    {
        let needs = self.data_table_state.as_mut().is_some_and(scroll);
        if !needs || !self.spawn_async_collect(Self::LOADING_BUFFER) {
            self.busy = false;
            self.status_message = None;
        }
        None
    }

    /// Hand the export modal's path input a key, and when the value changed, follow
    /// the typed extension with the format radio — the alternative was Parquet bytes
    /// in a file named `out.csv`, with nothing on screen saying so. Cursor-only keys
    /// change nothing and re-pick nothing, so a format chosen after typing stands.
    fn export_path_key(&mut self, event: &KeyEvent) {
        let before = self.export_modal.path_input.value().to_string();
        self.export_modal.path_input.handle_key(event, None);
        if self.export_modal.path_input.value() != before {
            self.export_modal.sync_format_to_path();
            // Typing is the correction the message asked for.
            self.export_modal.path_error = None;
        }
    }

    fn ensure_file_extension(
        path: &Path,
        format: ExportFormat,
        compression: Option<CompressionFormat>,
    ) -> PathBuf {
        let current_ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        let mut new_path = path.to_path_buf();

        if current_ext.is_empty() {
            // No extension: use default for format (and add compression if selected)
            let desired_ext = if let Some(comp) = compression {
                format!("{}.{}", format.extension(), comp.extension())
            } else {
                format.extension().to_string()
            };
            new_path.set_extension(&desired_ext);
        } else {
            // User provided an extension: keep it. Only add compression suffix when compression is selected.
            let is_compression_only = matches!(
                current_ext.to_lowercase().as_str(),
                "gz" | "zst" | "bz2" | "xz"
            ) && ExportFormat::from_extension(current_ext).is_none();

            if is_compression_only {
                // Path has only compression ext (e.g. file.gz); stem may have format (file.csv.gz)
                let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
                let stem_has_format = stem
                    .split('.')
                    .next_back()
                    .and_then(ExportFormat::from_extension)
                    .is_some();
                if stem_has_format {
                    if let Some(comp) = compression
                        && let Some(format_ext) = stem
                            .split('.')
                            .next_back()
                            .and_then(ExportFormat::from_extension)
                            .map(|f| f.extension())
                    {
                        new_path =
                            PathBuf::from(stem.rsplit_once('.').map(|x| x.0).unwrap_or(stem));
                        new_path.set_extension(format!("{}.{}", format_ext, comp.extension()));
                    }
                } else if let Some(comp) = compression {
                    new_path.set_extension(format!("{}.{}", format.extension(), comp.extension()));
                } else {
                    new_path.set_extension(format.extension());
                }
            } else if let Some(comp) = compression
                && format.supports_compression()
            {
                new_path.set_extension(format!("{}.{}", current_ext, comp.extension()));
            }
            // else: path stays as-is (e.g. foo.feather stays foo.feather)
            // else: path with format extension stays as-is
        }

        new_path
    }

    pub fn new(events: Sender<AppEvent>, runtime: tokio::runtime::Handle) -> App {
        // Create default theme for backward compatibility
        let theme = Theme::from_config(&AppConfig::default().theme).unwrap_or_else(|_| {
            // Create a minimal fallback theme
            Theme {
                colors: std::collections::HashMap::new(),
            }
        });

        Self::new_with_config(events, runtime, theme, AppConfig::default())
    }

    pub fn new_with_theme(
        events: Sender<AppEvent>,
        runtime: tokio::runtime::Handle,
        theme: Theme,
    ) -> App {
        Self::new_with_config(events, runtime, theme, AppConfig::default())
    }

    pub fn new_with_config(
        events: Sender<AppEvent>,
        runtime: tokio::runtime::Handle,
        theme: Theme,
        app_config: AppConfig,
    ) -> App {
        let cache = CacheManager::new(APP_NAME).unwrap_or_else(|_| CacheManager {
            cache_dir: std::env::temp_dir().join(APP_NAME),
        });

        let config_manager = ConfigManager::new(APP_NAME).unwrap_or_else(|_| ConfigManager {
            config_dir: std::env::temp_dir().join(APP_NAME).join("config"),
        });

        let template_manager = TemplateManager::new(&config_manager).unwrap_or_else(|_| {
            let temp_config = ConfigManager::new("datui").unwrap_or_else(|_| ConfigManager {
                config_dir: std::env::temp_dir().join("datui").join("config"),
            });
            TemplateManager::new(&temp_config).unwrap_or_else(|_| {
                let last_resort = ConfigManager {
                    config_dir: std::env::temp_dir().join("datui_config"),
                };
                TemplateManager::new(&last_resort)
                    .unwrap_or_else(|_| TemplateManager::empty(&last_resort))
            })
        });

        App {
            path: None,
            data_table_state: None,
            footer_progress: Arc::new(crate::schema_union::FooterProgress::default()),
            footers_this_frame: None,
            home: home::HomeState {
                hide_unreadable: !app_config.data.show_unreadable_files,
                ..Default::default()
            },
            home_probes_inflight: Vec::new(),
            #[cfg(feature = "cloud")]
            cloud_discovery_started: false,
            home_search_inflight: false,
            home_generation: 0,
            classify_inflight: None,
            classify_requests: 0,
            looking_at_directory: None,
            home_schema_inflight: Vec::new(),
            last_load_error: None,
            pending_clear_recents: false,
            pending_forget_place: None,
            home_schema_cache: HashMap::new(),
            original_file_format: None,
            original_file_delimiter: None,
            events,
            debug: DebugState::default(),
            info_modal: InfoModal::new(),
            parquet_metadata_cache: None,
            query_input: TextInput::new()
                .with_history_limit(app_config.query.history_limit)
                .with_theme(&theme)
                .with_history("query".to_string()),
            sql_input: TextInput::new()
                .with_history_limit(app_config.query.history_limit)
                .with_theme(&theme)
                .with_history("sql".to_string()),
            fuzzy_input: TextInput::new()
                .with_history_limit(app_config.query.history_limit)
                .with_theme(&theme)
                .with_history("fuzzy".to_string()),
            input_mode: InputMode::Normal,
            input_type: None,
            query_mode: QueryMode::default().resolve(),
            query_focus: QueryFocus::Input,
            sort_filter_modal: SortFilterModal::new(),
            pivot_melt_modal: PivotMeltModal::new(),
            template_modal: TemplateModal::new(),
            opened_from_home: false,
            startup_template: None,
            analysis_modal: AnalysisModal::with_sample_rows(
                app_config.performance.analysis_sample_rows,
            ),
            quality_cache: Vec::new(),
            quality_sample: None,
            quality_evidence_return: None,
            quality_evidence_label: None,
            reading_sample: false,
            chart_modal: ChartModal::new(),
            chart_export_modal: ChartExportModal::new(),
            export_modal: ExportModal::new(),
            copy_modal: copy_modal::CopyModal::new(),
            clipboard: None,
            pending_copy: None,
            chart_cache: ChartCache::default(),
            chart_inflight: None,
            chart_export_generation: 0,
            chart_export_inflight: None,
            pending_chart_result: Arc::new(Mutex::new(None)),
            chart_export_waiting: None,
            error_modal: ErrorModal::new(),
            flash: None,
            confirmation_modal: ConfirmationModal::new(),
            pending_export: None,
            export_df: None,
            pending_chart_export: None,
            #[cfg(any(feature = "http", feature = "cloud"))]
            pending_download: None,
            show_help: false,
            help_scroll: 0,
            cache,
            template_manager,
            active_template_id: None,
            loading_state: LoadingState::Idle,
            theme,
            pending_read_all: false,
            history_limit: app_config.query.history_limit,
            table_cell_padding: app_config.display.table_cell_padding.min(u16::MAX as usize) as u16,
            column_colors: app_config.display.column_colors,
            dtype_row: app_config.display.dtype_row,
            number_format: app_config
                .display
                .number_format
                .resolve(app_config.display.align_numeric_right)
                // AppConfig::load validates this, but App can be built from an
                // unvalidated config (e.g. the Python API): fall back to no
                // formatting while still honouring the alignment setting.
                .unwrap_or_else(|_| NumberFormatSettings {
                    align_numeric_right: app_config.display.align_numeric_right,
                    ..Default::default()
                }),
            runtime,
            task_generation: 0,
            load_active: false,
            awaiting_dataset: false,
            load_from_home: false,
            recent_on_install: None,
            opening: None,
            opened: None,
            #[cfg(any(feature = "http", feature = "cloud"))]
            downloaded_from: None,
            loaded_ahead_from: None,
            pending_lazyframe_result: Arc::new(Mutex::new(None)),
            pending_schema_result: std::sync::Arc::new(std::sync::Mutex::new(None)),
            pending_footers_result: std::sync::Arc::new(std::sync::Mutex::new(None)),
            dataset_generation: 0,
            footers_held: None,
            reread_owed: None,
            leases: HashMap::new(),
            collect_owed: None,
            end_when_the_footers_land: None,
            len_count_inflight: None,
            collect_inflight: None,
            len_count_failed: None,
            end_after_count: None,
            pending_collect_result: std::sync::Arc::new(std::sync::Mutex::new(None)),
            busy: false,
            throbber_frame: 0,
            screen_generation: 0,
            input_dropped: false,
            status_message: None,
            analysis_computation: None,
            app_config,
            #[cfg(any(feature = "http", feature = "cloud"))]
            http_temp_path: None,
        }
    }

    pub fn enable_debug(&mut self) {
        self.debug.enabled = true;
    }

    // ---- Home screen -----------------------------------------------------

    /// Schema for a home-screen entry, read from Parquet metadata and memoised for
    /// the session. `None` means "not knowable without a scan", which the UI reports
    /// rather than papering over.
    pub fn home_schema(&mut self, entry: &discover::Entry) -> Option<discover::SchemaPreview> {
        // A schema preview reads a local file. Nothing in an object store is read before
        // it is opened: asking would only come back empty, again on every rebuild.
        if home::is_cloud_place(&entry.path) || home::is_object_store_url(&entry.path) {
            return None;
        }
        if let Some(cached) = self.home_schema_cache.get(&entry.path) {
            return cached.clone();
        }
        // Reading a schema opens a file, so it is requested rather than done here.
        // Until it arrives the preview says so; it never blocks the frame.
        self.request_home_schema(entry.clone());
        None
    }

    /// Complete the path being typed, on a worker.
    fn request_path_completion(&mut self) {
        let typed = self.home.path_input.clone();
        if typed.is_empty() {
            return;
        }
        let generation = self.home_generation;
        let tx = self.events.clone();
        std::thread::spawn(move || {
            let (completed, candidates) = home::complete_path(&typed);
            let _ = tx.send(AppEvent::HomePathCompleted {
                generation,
                typed,
                completed,
                candidates,
            });
        });
    }

    /// Whether a schema read is currently out for this path.
    pub fn home_schema_pending(&self, path: &Path) -> bool {
        self.home_schema_inflight.iter().any(|p| p == path)
    }

    /// Read the selected dataset's schema on a worker.
    fn request_home_schema(&mut self, entry: discover::Entry) {
        if self.home_schema_inflight.contains(&entry.path) {
            return;
        }
        self.home_schema_inflight.push(entry.path.clone());

        let generation = self.home_generation;
        let tx = self.events.clone();
        self.runtime.spawn_blocking(move || {
            let preview = discover::schema_preview(&entry);
            let _ = tx.send(AppEvent::HomeSchemaReady {
                generation,
                path: entry.path,
                preview,
            });
        });
    }

    /// Start listing any network roots that have not answered yet.
    ///
    /// Nothing here waits on the result. A share that has gone away leaves its thread
    /// blocked in the kernel — on a `hard` NFS mount that is uninterruptible and the
    /// thread never returns — so the task is abandoned rather than joined, exactly as
    /// an abandoned dataset load is.
    fn spawn_home_probes(&mut self) {
        for root in self.home.pending_probes() {
            if self.home_probes_inflight.contains(&root) {
                continue;
            }
            // Each probe of an unreachable share costs a thread that will never come
            // back. A handful is a rounding error; an unbounded number, on a machine
            // with a page of dead mounts, is not.
            //
            // Except the directory browsed into, which is the whole screen and has
            // nothing else to show. Held behind the cap, it waited on roots the user
            // had left — a few slow bucket listings kept a share's directory on a
            // spinner long after it could have been read. One more thread per
            // directory the user opens is bounded by the user.
            let browsed = self.home.browsing.as_ref() == Some(&root);
            if !browsed && self.home_probes_inflight.len() >= MAX_CONCURRENT_PROBES {
                continue;
            }
            self.home_probes_inflight.push(root.clone());
            let tx = self.events.clone();
            let cache = self.cache.clone();
            #[cfg(feature = "cloud")]
            let cloud = self.app_config.cloud.clone();
            #[cfg(feature = "cloud")]
            let runtime = self.runtime.clone();
            // A detached OS thread, not the runtime's blocking pool. A thread wedged
            // on an unreachable `hard` mount never returns, and the pool is shared with
            // the work that actually loads data — a few dead shares must not eat into
            // the capacity that opening a dataset depends on.
            std::thread::spawn(move || {
                // A bucket or a prefix inside one. It looks like a network root to
                // everything above, and it is, but it is read with an object-store
                // listing rather than `read_dir` — which on a `gs://` path fails, which
                // is why descending into a bucket used to show nothing at all.
                //
                // Deliberately metadata-only. A delimited listing returns names, sizes
                // and modification times for one level, and nothing here reads an
                // object's contents: no footers, no schemas, no row counts. Those are
                // what a local listing fills in for free from bytes already on the
                // machine, and what would cost a ranged read per row against an object
                // store somebody pays egress on.
                #[cfg(feature = "cloud")]
                if let Some((id, account)) = home::cloud_account(&root) {
                    let listed = wait_on_runtime(&runtime, async move {
                        crate::cloud_browse::list_account(&id, &account, &cloud).await
                    });
                    match listed {
                        Some(Ok(rows)) => {
                            let _ = tx.send(AppEvent::HomeProbeReady {
                                root,
                                rows: Some(rows),
                                cut_short: false,
                            });
                        }
                        Some(Err(message)) => {
                            let _ = tx.send(AppEvent::HomeProbeFailed { root, message });
                        }
                        None => {
                            let _ = tx.send(AppEvent::HomeProbeReady {
                                root,
                                rows: None,
                                cut_short: false,
                            });
                        }
                    }
                    return;
                }
                #[cfg(feature = "cloud")]
                if crate::cloud_browse::split_bucket_url(&root.to_string_lossy()).is_some()
                    || source::azure_parts(&root.to_string_lossy()).is_some()
                {
                    let url = root.to_string_lossy().into_owned();
                    let listed = wait_on_runtime(&runtime, async move {
                        crate::cloud_browse::list_objects(&url, &cloud).await
                    });
                    // A refused listing says why, rather than reading as a place that
                    // stopped answering.
                    match listed {
                        Some(Err(message)) => {
                            let _ = tx.send(AppEvent::HomeProbeFailed { root, message });
                        }
                        other => {
                            let _ = tx.send(AppEvent::HomeProbeReady {
                                root,
                                rows: other.and_then(Result::ok),
                                cut_short: false,
                            });
                        }
                    }
                    return;
                }
                let mut cut_short = false;
                let rows = if std::fs::read_dir(&root).is_ok() {
                    // What has been read shows while the rest is read: a share can take
                    // seconds over a directory of thousands.
                    let scan = crate::discover::scan_dir_progressive(&root, |so_far| {
                        let _ = tx.send(AppEvent::HomeProbeProgress {
                            root: root.clone(),
                            rows: so_far.to_vec(),
                        });
                    });
                    cut_short = scan.truncated;
                    let mut rows = scan.entries;
                    // Measuring happens here too: it is the same remote filesystem,
                    // and this thread is already the one allowed to block on it.
                    for row in rows.iter_mut().take(PROBE_MEASURE_LIMIT) {
                        crate::discover::enrich(row);
                    }
                    // Remote datasets are measured nowhere else, so this is the only
                    // chance to remember them. Without it a remote row is blank on
                    // every run, which is exactly backwards: the hardest things to
                    // reach are the ones most worth remembering.
                    let mounts = crate::locality::Mounts::current();
                    for row in rows.iter_mut() {
                        row.cost.source = Some(mounts.describe(&row.path).fstype);
                    }
                    let facts: Vec<_> = rows.iter().filter_map(home::facts_for).collect();
                    cache.record_dataset_facts(&facts);
                    Some(rows)
                } else {
                    None
                };
                let _ = tx.send(AppEvent::HomeProbeReady {
                    root,
                    rows,
                    cut_short,
                });
            });
        }
    }

    /// Find the cloud sources this machine and the config describe, and list their
    /// buckets when `[cloud] list_on_start` asks. Once per session; Ctrl+R asks again.
    #[cfg(feature = "cloud")]
    fn spawn_cloud_discovery(&mut self) {
        if self.cloud_discovery_started {
            return;
        }
        self.cloud_discovery_started = true;
        let list = self.app_config.cloud.list_on_start == Some(true);
        self.list_cloud_sources(None, list);
    }

    /// List the source being browsed, when it has not been asked this session.
    /// Entering a source is the request to list it.
    #[cfg(feature = "cloud")]
    fn list_browsed_cloud_source(&mut self) {
        let Some(id) = self
            .home
            .browsing
            .as_deref()
            .and_then(home::cloud_source_id)
        else {
            return;
        };
        let Some(source) = self.home.cloud.iter_mut().find(|s| s.id == id) else {
            return;
        };
        if source.asked {
            return;
        }
        source.begin_listing();
        self.list_cloud_sources(Some(id), true);
    }

    /// Send the rows of every source, or list the buckets of the one named.
    ///
    /// The rows go out first, filled from the last run's listing when the source still
    /// points at the same place, so the home screen has its counts before any request
    /// is made. With `list`, the sources are then listed side by side, a few at a
    /// time, and each result is sent the moment it arrives. Without it nothing leaves
    /// the machine: no request, and no credential command.
    ///
    /// Runs on the runtime rather than a detached thread. Unlike a probe of a dead
    /// `hard` mount, an HTTP request cannot wedge forever: every call here is bounded
    /// by a global timeout, so the task is guaranteed to end.
    #[cfg(feature = "cloud")]
    fn list_cloud_sources(&mut self, only: Option<String>, list: bool) {
        let tx = self.events.clone();
        let cloud = self.app_config.cloud.clone();
        let cache = self.cache.clone();
        self.runtime.spawn(async move {
            let mut hidden = cache.load_hidden_cloud_sources();
            hidden.extend(cloud.hide.iter().cloned());
            let listings = cache.load_cloud_listings();
            let cached_for = |source: &crate::cloud_sources::Source| {
                listings
                    .get(&source.id)
                    .filter(|l| l.fingerprint == source.fingerprint())
            };
            let found = {
                let env = crate::cloud_browse::Environment::current();
                crate::cloud_sources::discover(&cloud, &env)
            };
            // Shown or not: a bucket under Recent opens with the login that listed it
            // whatever `discover` says. Not a hidden source, which may be hidden for a
            // login that no longer works; the default login opens its buckets instead.
            // Only with the rows, so an old listing never overrides one made since.
            if only.is_none() {
                for source in found.iter().filter(|s| !hidden.contains(&s.id)) {
                    if let Some(cached) = cached_for(source) {
                        crate::cloud_sources::remember_listed(source, &cached.buckets);
                    }
                }
            }
            let sources: Vec<crate::cloud_sources::Source> =
                crate::cloud_sources::on_home(found, &cloud)
                    .into_iter()
                    .filter(|s| !hidden.contains(&s.id))
                    .collect();

            match &only {
                None => {
                    let rows = sources
                        .iter()
                        .map(|source| home_cloud_source(source, cached_for(source), list))
                        .collect();
                    let _ = tx.send(AppEvent::HomeCloudSources { sources: rows });
                }
                // Gone since its row was drawn: a profile removed, a source hidden
                // elsewhere. Said, so the row does not wait on an answer never coming.
                Some(id) if !sources.iter().any(|s| &s.id == id) => {
                    let _ = tx.send(AppEvent::HomeCloudListed {
                        id: id.clone(),
                        buckets: Vec::new(),
                        details: Vec::new(),
                        failure: Some((
                            "not found".to_string(),
                            format!("{id} is gone or hidden. Ctrl+R at the top looks again."),
                        )),
                        listed_at: std::time::SystemTime::now(),
                    });
                    return;
                }
                Some(_) => {}
            }
            if !list {
                return;
            }

            // Enough to keep one slow endpoint from delaying the rest, few enough that a
            // long list of sources does not open a connection storm.
            const LISTING_AT_ONCE: usize = 4;
            let permits = std::sync::Arc::new(tokio::sync::Semaphore::new(LISTING_AT_ONCE));
            let mut listings = tokio::task::JoinSet::new();
            for source in sources
                .into_iter()
                .filter(|s| only.as_ref().is_none_or(|id| &s.id == id))
            {
                let permits = permits.clone();
                listings.spawn(async move {
                    let _permit = permits.acquire_owned().await;
                    let result = crate::cloud_browse::list_first_level(&source).await;
                    (source, result)
                });
            }
            while let Some(joined) = listings.join_next().await {
                let Ok((source, result)) = joined else {
                    continue;
                };
                let listed_at = std::time::SystemTime::now();
                // Buckets named in the config are shown whether or not the login can
                // list them; that is what naming them is for.
                let mut names = source.buckets.clone();
                let mut details = Vec::new();
                let failure = match result {
                    Ok(listed) => {
                        for item in listed {
                            crate::cloud_sources::remember_bucket(&source, &item.name);
                            if !item.details.is_empty() {
                                details.push((item.place.clone(), item.details));
                            }
                            let name = item.name;
                            if !names.contains(&name) {
                                names.push(name);
                            }
                        }
                        cache.save_cloud_listing(
                            &source.id,
                            crate::cache::CloudListing {
                                fingerprint: source.fingerprint(),
                                buckets: names.clone(),
                                listed_at: listed_at
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map(|d| d.as_secs())
                                    .unwrap_or(0),
                            },
                        );
                        None
                    }
                    Err(e) => Some(summarize_cloud_failure(&e)),
                };
                let _ = tx.send(AppEvent::HomeCloudListed {
                    id: source.id.clone(),
                    buckets: names
                        .iter()
                        .map(|b| PathBuf::from(source.bucket_url(b)))
                        .collect(),
                    details,
                    failure,
                    listed_at,
                });
            }
        });
    }

    /// Ask again for what is on screen, ignoring what is cached: the buckets of the
    /// source being browsed, the contents of the bucket or directory being browsed, or
    /// every source's buckets from the home listing.
    fn home_reload(&mut self) {
        #[cfg(feature = "cloud")]
        {
            let browsing = self.home.browsing.clone();
            match browsing.as_deref().and_then(home::cloud_source_id) {
                Some(id) => {
                    if let Some(source) = self.home.cloud.iter_mut().find(|s| s.id == id) {
                        source.begin_listing();
                    }
                    self.list_cloud_sources(Some(id), true);
                }
                None if browsing.is_none() && !self.home.cloud.is_empty() => {
                    for source in &mut self.home.cloud {
                        source.begin_listing();
                    }
                    self.list_cloud_sources(None, true);
                }
                None => {}
            }
        }
        if let Some(dir) = self.home.browsing.clone() {
            self.home.probed.remove(&dir);
            self.home.unreachable.remove(&dir);
            self.home.cut_short.remove(&dir);
        }
        // A peek that failed is asked again: Ctrl+R is the request to try.
        self.home.peek_failed.clear();
        self.home.status = None;
        self.home_refresh();
    }

    /// Start the recursive search below the working directory, if it is wanted and
    /// not already running.
    ///
    /// Triggered by typing rather than by opening the home screen: typing is the
    /// signal that someone is looking for something. Launching datui, pressing Enter
    /// on a recent dataset and leaving costs no walk at all.
    fn spawn_home_search(&mut self) {
        if self.home_search_inflight || self.home.search.done {
            return;
        }
        let config = self.app_config.data.search.clone();
        if !config.enabled {
            return;
        }
        let Some(root) =
            crate::search::search_root(self.home.browsing.as_ref(), self.home.network_check)
        else {
            return;
        };

        self.home.search.reset();
        self.home.search.root = Some(root.clone());
        self.home.search.running = true;
        self.home_search_inflight = true;

        let generation = self.home_generation;
        let tx = self.events.clone();
        // A detached thread for the same reason the probes use one: the walk touches
        // a filesystem, and nothing that touches a filesystem may run where a stall
        // would stop the screen from drawing.
        std::thread::spawn(move || {
            let walk_root = root.clone();
            let batch_tx = tx.clone();
            let batch_gen = generation;
            let batch_root = root.clone();
            let outcome = crate::search::walk(&walk_root, &config, move |found, outcome| {
                // Sent even when empty: it carries the progress count, and it is the
                // only place the walk learns that nobody is listening any more.
                batch_tx
                    .send(AppEvent::HomeSearchBatch {
                        generation: batch_gen,
                        root: batch_root.clone(),
                        found,
                        scanned: outcome.scanned,
                    })
                    // A closed channel means the app is gone; stop walking.
                    .is_ok()
            });
            let _ = tx.send(AppEvent::HomeSearchDone {
                generation,
                root,
                scanned: outcome.scanned,
                limited: outcome.note().map(str::to_string),
            });
        });
    }

    /// Rebuild the home listing from the filesystem.
    fn home_refresh(&mut self) {
        // Every way into a source comes through here: Enter, Backspace up from a
        // bucket, a jump, and rows arriving while the source is already open.
        #[cfg(feature = "cloud")]
        self.list_browsed_cloud_source();
        self.home_generation = self.home_generation.wrapping_add(1);
        let generation = self.home_generation;

        // Reading recents touches only the cache directory, which is local by
        // definition; everything that might block happens on the worker.
        let recents = self.cache.load_recents();
        self.home.collections = home::collections(&self.app_config);
        // The same facts the listing is annotated from, kept on the home state so
        // rows the recursive search finds can be filled in the same way.
        self.home.known = self.cache.load_dataset_facts();
        let request = home::ListingRequest {
            config_dirs: self.app_config.data.resolved_directories(),
            remembered_dirs: self.cache.load_remembered_places(),
            recents,
            desktop_dirs: if self.app_config.data.use_desktop_recents {
                home::desktop_recent_dirs()
            } else {
                Vec::new()
            },
            browsing: self.home.browsing.clone(),
            probed: self.home.probed.clone(),
            unreachable: self.home.unreachable.clone(),
            listing_so_far: self.home.listing_so_far.clone(),
            cut_short: self.home.cut_short.clone(),
            probe_errors: self.home.probe_errors.clone(),
            network_check: self.home.network_check,
            cloud: self.home.cloud.clone(),
            collections: self.home.collections.clone(),
            known: self.home.known.clone(),
        };

        self.home.listing_in_flight = true;
        let tx = self.events.clone();
        self.runtime.spawn_blocking(move || {
            let listing = home::build_listing(&request);
            let _ = tx.send(AppEvent::HomeListingReady {
                generation,
                listing: Box::new(listing),
            });
        });
    }

    /// Ask the worker to measure rows that are on screen and not yet known.
    ///
    /// Reading a Parquet footer opens a file. That is the call that blocks on a FIFO,
    /// a device node, a wedged mount or a failing disk, so it never happens on the
    /// thread that draws.
    fn request_home_measurements(&mut self) {
        if self.home.measure_in_flight {
            return;
        }
        let wanted = self.home.unmeasured_visible(MEASURE_BATCH);
        if wanted.is_empty() {
            return;
        }

        self.home.measure_in_flight = true;
        let tx = self.events.clone();
        let cache = self.cache.clone();
        self.runtime.spawn_blocking(move || {
            home::look_into_batch(wanted, &cache, |path, m| {
                let _ = tx.send(AppEvent::HomeMeasured {
                    measured: vec![(path, m)],
                    done: false,
                });
            });
            let _ = tx.send(AppEvent::HomeMeasured {
                measured: Vec::new(),
                done: true,
            });
        });
    }

    /// Ask for what the frame just drawn needs and did not have: counts for the rows
    /// on screen that have none, and kinds for the rows nothing has looked into.
    ///
    /// After the frame, never during it. Scrolling is what brings new rows into view,
    /// and the reading is a worker's job — this thread only decides what is worth
    /// asking about.
    pub fn request_what_the_frame_needs(&mut self) {
        if self.input_mode == InputMode::Normal {
            self.load_ahead();
        }
        if self.input_mode != InputMode::Home {
            return;
        }
        if std::mem::take(&mut self.home.pending_enrich) {
            self.request_home_measurements();
        }
        if std::mem::take(&mut self.home.pending_classify) {
            self.request_home_classifications();
        }
        #[cfg(feature = "cloud")]
        if std::mem::take(&mut self.home.pending_peek) {
            self.peek_cloud_directories();
        }
    }

    /// Ask a worker what the rows on screen are.
    ///
    /// Classifying a row means reading the directory it names, which on a share is a
    /// round trip and on a wedged mount never returns — so it happens here rather
    /// than while the listing is built, where it was paid for in directory order and
    /// bought a label for the first sixty-four rows and a wrong one for the rest.
    ///
    /// A detached thread, not the runtime's blocking pool, for the reason the probes
    /// give: a thread stuck on an unreachable `hard` mount never comes back, and the
    /// pool is shared with the work that actually loads data. One at a time, so a
    /// share that has stopped answering costs one thread and then stops asking.
    ///
    /// This measures remote rows as well as classifying them, which
    /// [`HomeState::unmeasured_visible`] deliberately refuses to do — it leaves them to
    /// their root's probe, so that a share gets one thread and not two. That reasoning
    /// no longer reaches: the probe scans a remote directory before anything has looked
    /// into it, so every row it returns is `Unknown` and there is nothing for it to
    /// measure. This pass is the only thing left that can, and it makes the same bargain
    /// the probe made — one detached thread, on a filesystem it is already reading.
    fn request_home_classifications(&mut self) {
        if self.home.classify_in_flight {
            return;
        }
        let wanted = self.home.unclassified_visible(CLASSIFY_BATCH);
        if wanted.is_empty() {
            return;
        }

        self.home.classify_in_flight = true;
        let tx = self.events.clone();
        let cache = self.cache.clone();
        std::thread::spawn(move || {
            home::look_into_batch(wanted, &cache, |path, m| {
                let _ = tx.send(AppEvent::HomeClassified {
                    measured: vec![(path, m)],
                    done: false,
                });
            });
            let _ = tx.send(AppEvent::HomeClassified {
                measured: Vec::new(),
                done: true,
            });
        });
    }

    /// Enter the home screen, rebuilding it, abandoning any in-flight load.
    ///
    /// Returning home puts the cursor on whatever you currently have open, so the
    /// round trip out and back lands where you left rather than at the top.
    ///
    /// Abandoning is `load_active = false` plus clearing the load's own UI state,
    /// and cancelling the load's footer counter, so an in-flight cloud pass stops
    /// issuing paid reads within a wave. Other background work runs to completion
    /// and its results are dropped on arrival. Work that is not a load — an
    /// export, an analysis — is deliberately left alone, so its progress
    /// indicator and its completion modal must survive this.
    pub fn abandon_load(&mut self) {
        // Only the in-flight load's counter: with no load running, this counter
        // belongs to the installed dataset's own background pass, which a trip
        // home must not stop.
        if self.load_active {
            self.footer_progress.cancel();
        }
        self.load_active = false;
        // A chart being prepared for the dataset we are leaving would otherwise keep
        // the throbber up on the home screen, and its result could later land in a
        // different dataset with the same column names.
        self.reset_chart_state();
        // A look that is out belongs to the home screen being left, and the thread it is
        // on may never come back — a share that has gone away is the case it exists for.
        // Its answer will find nothing outstanding and touch nothing; the keyboard does
        // not wait for it.
        if self.classify_inflight.take().is_some() {
            self.busy = false;
            self.home.status = None;
        }
        // And the look at a directory named on the command line, for the same reason: it
        // takes seconds, Ctrl+O works throughout, and its answer must not take the user
        // off the screen they went to instead.
        if self.looking_at_directory.take().is_some() {
            self.busy = false;
            if self.status_message.as_deref() == Some(Self::LOOKING_AT_A_DIRECTORY) {
                self.status_message = None;
            }
        }
        // Nothing is arriving to replace it, so the dataset already on screen is the
        // current one again — Esc from home goes straight back to it.
        self.awaiting_dataset = false;
        self.load_from_home = false;
        self.recent_on_install = None;
        self.opening = None;
        // And a collect that was waiting behind this load goes with it. Left standing,
        // it runs the moment the load's lease comes back — reading the dataset the user
        // walked away from, at the home screen, with `busy` set and every key held.
        self.collect_owed = None;
        #[cfg(any(feature = "http", feature = "cloud"))]
        if self.pending_download.take().is_some() {
            self.confirmation_modal.hide();
        }
        // Only a load's own busy state is cleared. An export sets `busy` and owns
        // `loading_state` too, and it keeps running.
        if matches!(self.loading_state, LoadingState::Loading { .. }) {
            self.loading_state = LoadingState::Idle;
            self.busy = false;
        }
        // Keys typed at the frozen screen were meant for the load, not for home:
        // replayed there they could open a dataset nobody asked for.
        self.screen_generation = self.screen_generation.wrapping_add(1);
    }

    pub fn enter_home(&mut self) {
        if self.return_from_quality_evidence(false) {
            self.analysis_modal.close();
        }
        // The template modal keys and renders off its own `active`, not the input
        // mode, so left open here it would come back as a zombie over the next
        // dataset opened.
        self.template_modal.close();
        self.abandon_load();
        self.home.status = None;
        self.home.folds = self.cache.load_folds();
        self.home_refresh();
        if let Some(open_path) = self.path.clone() {
            let target =
                crate::canonical::canonicalize(&open_path).unwrap_or_else(|_| open_path.clone());
            if let Some(idx) = self.home.visible().iter().position(|row| match row {
                home::Row::Entry { entry, .. } => {
                    crate::canonical::canonicalize(&entry.path)
                        .unwrap_or_else(|_| entry.path.clone())
                        == target
                }
                // Not the door: its path is the directory's, so an open file whose
                // directory is being browsed would put the cursor on the row that
                // opens the whole directory rather than on the file itself.
                home::Row::Header { .. }
                | home::Row::Place { .. }
                | home::Row::More { .. }
                | home::Row::Hidden { .. }
                | home::Row::Door { .. } => false,
            }) {
                self.home.selected = idx;
            }
        }
        self.input_mode = InputMode::Home;
    }

    /// Esc backs out one layer of context at a time: the filter, then the directory
    /// descended into, then back to the data that was open. At the top level it does
    /// nothing. It used to quit there, which made a reflexive Esc close the program
    /// while the same key one level down merely went up; Ctrl+C quits, from anywhere.
    fn home_escape(&mut self) -> Option<AppEvent> {
        if !self.home.filter.is_empty() {
            self.home.filter.clear();
            self.home.sync_search_section();
            self.home.selected = 0;
            self.home.clamp_selection();
            return None;
        }
        if self.home.browsing.is_some() {
            if self.home.below_browse_start() {
                self.home_ascend();
            } else {
                // Climbing past where the browse began would take Esc somewhere the
                // user never was; it returns to the listing they started from instead.
                self.home_leave_browsing(None);
            }
            return None;
        }
        if self.data_table_state.is_some() {
            self.input_mode = InputMode::Normal;
        }
        None
    }

    /// Drop the highlighted dataset from the recents list.
    ///
    /// Only from the Recent section: a row under a directory is a file on disk, and
    /// forgetting it there would either do nothing or imply a deletion datui is not
    /// going to perform.
    fn home_forget_selected(&mut self) {
        // A place row stands for every recent under it. Forgetting them all is one
        // keystroke from forgetting one, so it asks first, the way Shift+Delete does.
        if let Some(home::Row::Place { path, held, .. }) = self.home.selected_row() {
            self.pending_forget_place = Some(path.clone());
            self.confirmation_modal.show(format!(
                "Forget {held} recently opened {} under {}?",
                if held == 1 { "dataset" } else { "datasets" },
                home::display_path(&path)
            ));
            return;
        }
        // The heading of a remembered place stands for the place. No question first:
        // Ctrl+D puts it back. A row under it is a file on disk, as anywhere else.
        if self.home.browsing.is_none()
            && let Some(section) = self.home.selected_section().map(|i| &self.home.sections[i])
            && let Some(root) = section.root.clone()
        {
            let header = self.home.selection_is_header();
            match section.origin {
                Some(o) if o == home::RootOrigin::Remembered.note() => {
                    if header {
                        self.home_set_remembered(&root, false);
                    } else {
                        self.home.status = Some(format!(
                            "Delete on the heading forgets {}",
                            home::display_path(&root)
                        ));
                    }
                    return;
                }
                Some(o) if o == home::RootOrigin::Configured.note() && header => {
                    self.home.status = Some(Self::configured_place_note(&root));
                    return;
                }
                _ => {}
            }
        }
        let section_title = self
            .home
            .selected_section()
            .and_then(|i| self.home.sections.get(i))
            .map(|s| s.title.clone())
            .unwrap_or_default();
        if self.home.browsing.is_none()
            && section_title == home::HomeState::CLOUD_SECTION
            && let Some(id) = self
                .home
                .selected_entry()
                .and_then(|e| home::cloud_source_id(&e.path))
        {
            self.cache.hide_cloud_source(&id);
            self.home.cloud.retain(|s| s.id != id);
            self.home_refresh();
            return;
        }
        let in_recents = section_title == "Recent";
        if !in_recents {
            self.home.status = Some("Only recents and remembered places can be forgotten".into());
            return;
        }
        let Some(entry) = self.home.selected_entry() else {
            return;
        };
        self.cache.forget_recent(&entry.path);
        // Nothing to say: the row going is the answer.
        self.home.status = None;
        self.home_refresh();
    }

    /// The directory the highlighted row stands for, as Ctrl+D sees it: a directory
    /// row is itself, a file is the directory it is in, and a heading is the directory
    /// its section lists.
    fn home_place_under_cursor(&self) -> Option<PathBuf> {
        match self.home.selected_row()? {
            home::Row::Place { path, .. } => Some(path),
            home::Row::Door { entry, .. } => Some(entry.path.clone()),
            home::Row::Entry { entry, .. } => match entry.kind {
                discover::EntryKind::File | discover::EntryKind::Other => {
                    entry.path.parent().map(Path::to_path_buf)
                }
                _ => Some(entry.path.clone()),
            },
            home::Row::Header { section, .. } => self.home.sections.get(section)?.root.clone(),
            home::Row::More { .. } | home::Row::Hidden { .. } => None,
        }
    }

    /// How a place is compared and stored: resolved when it is local, as spelled when
    /// it is remote, since resolving a path on a share that has stopped answering is
    /// the stat that hangs. The door's trailing slash goes either way.
    fn place_key(&self, path: &Path) -> PathBuf {
        if (self.home.network_check)(path) {
            path.components().collect()
        } else {
            canonical::canonicalize(path).unwrap_or_else(|_| path.components().collect())
        }
    }

    fn configured_place_note(path: &Path) -> String {
        format!(
            "{} is in [data] directories; edit the config to remove it",
            home::display_path(path)
        )
    }

    /// Ctrl+D: keep the place under the cursor on the home screen, or stop keeping it.
    fn home_toggle_remembered(&mut self) {
        let Some(path) = self.home_place_under_cursor() else {
            self.home.status = Some("Move to a directory to remember it".into());
            return;
        };
        // Roots are directories on a filesystem. A bucket already has its source's
        // row, and an HTTP place has nothing to list.
        if home::is_object_store_url(&path)
            || home::is_cloud_place(&path)
            || !matches!(source::input_source(&path), source::InputSource::Local(_))
        {
            self.home.status = Some("Only directories on a filesystem can be remembered".into());
            return;
        }
        let key = self.place_key(&path);
        let configured = self
            .app_config
            .data
            .resolved_directories()
            .iter()
            .any(|dir| self.place_key(dir) == key);
        if configured {
            self.home.status = Some(Self::configured_place_note(&key));
            return;
        }
        let remembered = self.cache.load_remembered_places().contains(&key);
        self.home_set_remembered(&key, !remembered);
    }

    fn home_set_remembered(&mut self, place: &Path, keep: bool) {
        if keep {
            self.cache.remember_place(place);
        } else {
            self.cache.forget_place(place);
        }
        let verb = if keep { "Remembered" } else { "Forgot" };
        self.flash_note(format!("{verb} {}", home::display_path(place)));
        self.home_refresh();
    }

    /// Collapse or expand the section the cursor is in.
    ///
    /// Collapsing moves the cursor to the header, so the section the user just folded
    /// is what stays selected rather than whatever row happens to fall into place.
    /// Fold or unfold the section whose header is highlighted.
    fn home_toggle_fold(&mut self) {
        if let Some(section) = self.home.selected_section() {
            self.home.toggle_collapsed(section);
            self.home.clamp_selection();
            self.cache.save_folds(&self.home.folds);
        }
    }

    fn home_collapse(&mut self, collapse: bool) {
        // The listing browsed into is the whole screen. It never folds, and the fold
        // must not be remembered for its path either — see `set_collapsed`.
        if self.home.browsing.is_some() {
            return;
        }
        let Some(section) = self.home.selected_section() else {
            return;
        };
        if collapse && !self.home.is_collapsed(section) {
            self.home.set_collapsed(section, true);
            if let Some(idx) = self
                .home
                .visible()
                .iter()
                .position(|row| row.section() == section)
            {
                self.home.selected = idx;
            }
        } else if !collapse {
            self.home.set_collapsed(section, false);
        }
        self.home.clamp_selection();
        self.cache.save_folds(&self.home.folds);
    }

    /// Step out of a directory that was descended into.
    fn home_ascend(&mut self) {
        let Some(current) = self.home.browsing.clone() else {
            return;
        };
        let parent = self.home.parent_of(&current);
        self.home_leave_browsing(parent);
    }

    /// Move the browse up to `to`, or back to the root listing when `None`.
    fn home_leave_browsing(&mut self, to: Option<PathBuf>) {
        // Whatever the last place said about itself, it said about that place. "these
        // are the files under it" is wrong the moment "it" is somewhere else.
        self.home.status = None;
        self.home.browsing = to;
        // Backspace can climb above where the browse began; the start follows, so a
        // later Esc still has a place to stop.
        if !self.home.below_browse_start() {
            self.home.browse_start = self.home.browsing.clone();
        }
        // Going up widens what a search would cover, so the previous one no longer
        // answers the question being asked.
        self.home.search.reset();
        self.home.selected = 0;
        self.home_refresh();
    }

    /// Whether a peek's answer changes anything a row draws.
    ///
    /// Every answer that tells a row something, not only the ones that change the kind:
    /// a prefix of twelve CSV objects is a `Directory` — only Parquet is read in place —
    /// and it is still `12 csv`, which is the count the row is labelled from.
    ///
    /// An answer that says neither is replaced by "a directory, and nothing to say
    /// about it" rather than dropped. The directory still has to come back — that is what
    /// takes it out of `peeking` and holds the one-request-per-directory promise — and
    /// once the request has been made, "nothing to say" is a real answer rather than
    /// the claim it was when it was being written before the request. What this decides
    /// is whether the peek's own words are kept.
    ///
    /// "Says something" is `Holds::is_empty`, not the formats alone. The row is not the
    /// only thing an answer reaches: the details pane draws the whole `holds` line, so a
    /// prefix of a README and two PDFs has `3 not read` to report, and one of twelve
    /// sub-prefixes has `12 directories`. Testing the formats dropped both, and the same
    /// directories on disk said both things.
    ///
    /// The cost is real and is the reason the distinction is kept: each batch rebuilds
    /// the listing on the thread drawing the frame. `Holds::is_empty` is the line
    /// because it is the same question the pane asks before drawing the line at all.
    ///
    /// A `Holds` whose only field is `truncated` is let through too: it is what turns
    /// a cloud row's `dir` into `dir+`.
    #[cfg(feature = "cloud")]
    fn peek_tells_a_row_something(answer: &(discover::EntryKind, discover::Holds)) -> bool {
        answer.0 != discover::EntryKind::Directory || !answer.1.is_empty()
    }

    /// Look inside the cloud directories the cursor is on or near, so the ones that are
    /// datasets say `hive` or `multi` and open as one. One small listing request per
    /// directory, and each directory is peeked at once per session.
    ///
    /// A directory the listing takes for `multi` costs a little more: up to three ranged
    /// reads of a few kilobytes each, to ask the footers whether its files are really
    /// one table. Nothing else reads an object, and nothing reads a whole one.
    ///
    /// Driven by the cursor rather than by the listing. It used to take the first
    /// forty-eight directories of each listing, once: a bucket of two hundred prefixes
    /// had forty-eight labelled and the rest reading `dir` for the session however long
    /// you spent on them, and paging straight past those forty-eight spent the requests
    /// on rows nobody saw. The budget is the same shape as the local classify pass now —
    /// what is on screen, a batch at a time, the highlighted row first.
    #[cfg(feature = "cloud")]
    fn peek_cloud_directories(&mut self) {
        const PEEKS_AT_ONCE: usize = 4;
        let directories = self.home.cloud_directories_to_peek(PEEKS_AT_ONCE);
        if directories.is_empty() {
            return;
        }
        // Out, not answered. A second pass before these land must not ask again, and an
        // answer written here instead would be a claim — `dir` on a row that has a
        // count, and "never again this session" staked on a request that may fail.
        for directory in &directories {
            self.home.peeking.insert(directory.clone());
        }
        let tx = self.events.clone();
        let cloud = self.app_config.cloud.clone();
        self.runtime.spawn(async move {
            let permits = Arc::new(tokio::sync::Semaphore::new(PEEKS_AT_ONCE));
            let mut peeks = tokio::task::JoinSet::new();
            // By task, so a peek that panics is still sent back, as failed, and does
            // not stay in `peeking` spinning for good.
            let mut asked = std::collections::HashMap::new();
            for directory in directories {
                let (permits, cloud) = (permits.clone(), cloud.clone());
                let task_directory = directory.clone();
                let task = peeks.spawn(async move {
                    let directory = task_directory;
                    let _permit = permits.acquire_owned().await;
                    let kind =
                        crate::cloud_browse::peek_kind(&directory.to_string_lossy(), &cloud).await;
                    (directory, kind)
                });
                asked.insert(task.id(), directory);
            }
            // Sent a few at a time: the labels fill in as they are found, without a
            // rebuild per directory.
            //
            // Every directory asked about is sent back, including the ones whose peek
            // decided nothing and the ones whose request failed. That is what takes
            // them out of `peeking` and what holds the one-request-per-directory promise
            // — and an answer of "a directory, and nothing to say about it" is a real
            // answer once the request has been made, which is what it was not while it
            // was being written before the request.
            let mut found = Vec::new();
            let mut failed = Vec::new();
            while let Some(joined) = peeks.join_next_with_id().await {
                match joined {
                    Ok((_, (directory, Ok(answer)))) => {
                        let answer = Some(answer)
                            .filter(Self::peek_tells_a_row_something)
                            .unwrap_or((discover::EntryKind::Directory, Default::default()));
                        found.push((directory, answer));
                    }
                    Ok((_, (directory, Err(_)))) => failed.push(directory),
                    Err(error) => failed.extend(asked.remove(&error.id())),
                }
                if found.len() + failed.len() >= PEEKS_AT_ONCE {
                    let _ = tx.send(AppEvent::HomeCloudKinds {
                        kinds: std::mem::take(&mut found),
                        failed: std::mem::take(&mut failed),
                    });
                }
            }
            if !found.is_empty() || !failed.is_empty() {
                let _ = tx.send(AppEvent::HomeCloudKinds {
                    kinds: found,
                    failed,
                });
            }
        });
    }

    /// Browse into a directory or bucket, local or remote.
    fn home_browse_into(&mut self, path: PathBuf) {
        if self.home.browsing.is_none() {
            self.home.browse_start = Some(path.clone());
        } else if self.home.browse_start.is_none() {
            self.home.browse_start = self.home.browsing.clone();
        }
        self.home.browsing = Some(path);
        // "Below here" now means somewhere else. Whatever the last walk found
        // describes a different place, and a fresh one starts on the next
        // keystroke. The status line goes with them: "these are the files under it"
        // is about wherever "it" was. A caller with something to say about the place
        // it is going says it after this returns.
        self.home.status = None;
        self.home.search.reset();
        self.home.filter.clear();
        self.home.sync_search_section();
        self.home.selected = 0;
        self.home_refresh();
    }

    /// Whether the highlighted row is the `(all files)` row: the one that opens the
    /// directory being browsed, and so is already inside it.
    ///
    /// `Enter` on it opens the directory whatever the label says, and → on it would
    /// descend into where it already is.
    /// Asked of the row's variant rather than of a flag on the entry it carries: the
    /// door is a `Row::Door` now, so this is one match instead of a clone.
    fn selection_opens_the_whole_directory(&self) -> bool {
        self.home.selection_is_the_door()
    }

    /// The highlighted row, when → goes inside it.
    ///
    /// Every directory, whatever its label. A label describes what is directly inside; it
    /// no longer decides what can be reached, so the exception list this used to carry —
    /// hive, multi and the three lake markers — is gone, and with it the directories that
    /// had no way in because datui did not recognize how they were stored. What is left
    /// out is what is not a directory: a file, a section header, and the row that opens
    /// the directory you are already in.
    ///
    /// Local or remote. The split this used to carry — remote only — was never about
    /// where the directory was: a cloud prefix simply could not be descended into until
    /// there was a listing to descend with.
    fn selected_directory_to_enter(&self) -> Option<PathBuf> {
        // A place under `RECENT` is a directory to go inside, and → is one of its two
        // doors. It has no entry to ask about, so it is answered before one is looked for.
        if let Some(home::Row::Place { path, .. }) = self.home.selected_row() {
            return home::place_is_browsable(&path).then_some(path);
        }
        let entry = self.home.selected_entry()?;
        if self.selection_opens_the_whole_directory() || self.home.missing.contains(&entry.path) {
            return None;
        }
        (!matches!(
            entry.kind,
            discover::EntryKind::File | discover::EntryKind::Other
        ))
        .then_some(entry.path)
    }

    /// Why a prefix in an object store cannot be read as one table, when it cannot.
    ///
    /// Every cloud path is scanned as Parquet — the directory-format dispatch is local
    /// only — so a prefix of anything else comes back "Could not read from S3. Check
    /// credentials and URL", which is a false statement about a login that is fine.
    /// What the prefix holds is already counted and on screen, so saying so costs no
    /// request. #275 phase 4 is where these read.
    ///
    /// `None` for a prefix that may yet be Parquet: one holding Parquet, and one
    /// holding no data files at all, whose data may be a level down.
    /// Why Enter on a bucket directory's `(all files)` row reads nothing, by the rule
    /// Enter itself applies: a hive root or a directory of one table is read through
    /// its files, and one with a reader for what it holds is read with that.
    #[cfg(feature = "cloud")]
    pub(crate) fn why_a_door_reads_nothing(entry: &discover::Entry) -> Option<String> {
        if !home::is_object_store_url(&entry.path)
            || matches!(
                entry.kind,
                discover::EntryKind::Hive | discover::EntryKind::MultiFile
            )
            || Self::cloud_prefix_format(&entry.holds).is_some()
        {
            return None;
        }
        Self::why_a_cloud_prefix_cannot_be_read(&entry.holds)
    }

    #[cfg(feature = "cloud")]
    fn why_a_cloud_prefix_cannot_be_read(holds: &discover::Holds) -> Option<String> {
        let reads_parquet =
            |name: &str| crate::FileFormat::from_name(name) == Some(crate::FileFormat::Parquet);
        if holds.formats.iter().any(|(name, _)| reads_parquet(name)) {
            return None;
        }
        match holds.formats.as_slice() {
            // Data files, none of them Parquet. `label()` says `mixed` for more than
            // one format, which is a word rather than a count, so the line is spelled
            // out from the formats themselves.
            [] => {
                // Nothing datui has a reader for. Only a refusal when there is also
                // nothing below: a prefix of sub-prefixes may hold Parquet a level
                // down, and nothing here has looked.
                (holds.not_read > 0 && holds.directories == 0).then(|| {
                    "this prefix holds nothing datui can read — datui reads a directory in \
                     an object store as Parquet only."
                        .to_string()
                })
            }
            formats => {
                let held = formats
                    .iter()
                    .map(|(name, count)| format!("{count} {name}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                Some(format!(
                    "this prefix holds {held} — datui reads a directory in an object store \
                     as Parquet only. Open one of the files below instead."
                ))
            }
        }
    }

    /// The reader a prefix in an object store calls for, from what its listing counted.
    ///
    /// The commonest format, which is the same rule a directory on disk follows — and
    /// `rank_formats` is the same order, so a prefix and the directory it mirrors pick
    /// the same reader. `None` when nothing there has a multi-file reader, which is where
    /// the refusal that names what is there belongs.
    ///
    /// Parquet included and returned as itself: the cloud branches compare against it
    /// and take their own path, which is the one every cloud dataset took before any of
    /// this, and the only one with hive partitioning behind it.
    #[cfg(feature = "cloud")]
    fn cloud_prefix_format(
        holds: &discover::Holds,
    ) -> Option<(FileFormat, Vec<(FileFormat, usize)>)> {
        let (name, _) = holds.formats.first()?;
        let format = FileFormat::from_name(name).filter(|f| f.reads_many_files())?;
        // And what taking the commonest passes over. The local read reports its own —
        // it is the pass that decides — but here Polars does the listing and never sees
        // the other formats, so the note has to be written from the listing on screen.
        let left_out = holds
            .formats
            .iter()
            .skip(1)
            .filter_map(|(name, n)| FileFormat::from_name(name).map(|f| (f, *n)))
            .collect();
        Some((format, left_out))
    }

    /// Open the highlighted entry: toggle a section, descend into a directory, or
    /// load a dataset.
    fn home_open_selected(&mut self) -> Option<AppEvent> {
        match self.home.selected_row() {
            // Into the directory or prefix the recents under it live in: the way back
            // to a place found by hand, now that recents no longer make roots.
            Some(home::Row::Place { path, .. }) => {
                if home::place_is_browsable(&path) {
                    self.home_browse_into(path);
                } else {
                    self.home.status = Some(
                        "An HTTP server has no listing to browse. Open a file under it".into(),
                    );
                }
                return None;
            }
            // The rest of `RECENT`, for the session.
            Some(home::Row::More { .. }) => {
                self.home.recent_expanded = true;
                return None;
            }
            // What Ctrl+A shows. The cursor goes to the first of them, where the row
            // that stood for them was.
            Some(home::Row::Hidden { .. }) => {
                self.home.hide_unreadable = false;
                if let Some(idx) = self.home.visible().iter().position(|row| {
                    matches!(row, home::Row::Entry { entry, .. }
                        if entry.kind == discover::EntryKind::Other)
                }) {
                    self.home.selected = idx;
                }
                return None;
            }
            _ => {}
        }
        if self.home.selection_is_header() {
            self.home_toggle_fold();
            return None;
        }
        let entry = self.home.selected_entry()?;
        // A collection's local dataset that is not there: said here, where it was named.
        if self.home.missing.contains(&entry.path) {
            self.home.status = Some(format!(
                "{} does not exist",
                home::display_path(&entry.path)
            ));
            return None;
        }
        // The `(all files)` row opens the directory it names, whatever the directory is
        // labelled. That is the whole of what it is for: the label describes, and this
        // row is the promise that the description cannot lock you out. Sent straight to
        // the open, because `open_what_it_is` would read the label back and send a
        // `dir` row inside the directory it is already in.
        if self.selection_opens_the_whole_directory() {
            // A lake table is not a directory of Parquet files however much it looks like
            // one: reading it as one counts tombstoned rows, every rewritten version
            // and both sides of a compaction. So the read is labelled rather than
            // refused. Refusing it left a directory the user could see and could not read
            // at all — this row is the promise that no label locks you out, and a
            // refusal here is that promise broken on the one directory that needed it.
            // Until datui reads the log, its files are what there is, and what makes
            // that honest is that nothing about it is silent: a note in the panel, a
            // chip in the control bar, and `Enter` on the row one level up still goes
            // inside and says datui does not read the table itself yet.
            let lake = entry.kind.lake_name();
            // A prefix in an object store used to be scanned as Parquet whatever was
            // in it — every cloud path returns before the directory-format dispatch is
            // reached — so a prefix of CSV answered "Could not read from S3. Check
            // credentials and URL", a false statement about the user's login. What the
            // prefix holds was counted by the listing and is on screen, so the reader
            // is picked from it, which costs no request. Only a prefix the listing
            // already calls a dataset is left alone: a hive root is read through its
            // partitions, and one stray `manifest.csv` beside them is not what it
            // holds — but it is the only thing in `formats`.
            #[cfg(feature = "cloud")]
            let reader = if home::is_object_store_url(&entry.path)
                && !matches!(
                    entry.kind,
                    discover::EntryKind::Hive | discover::EntryKind::MultiFile
                ) {
                let reader = Self::cloud_prefix_format(&entry.holds);
                // Nothing here datui has a reader for. The listing is on screen, so the
                // refusal names what is there rather than blaming the connection.
                if reader.is_none()
                    && let Some(what) = Self::why_a_cloud_prefix_cannot_be_read(&entry.holds)
                {
                    self.home.status = Some(what);
                    return None;
                }
                reader
            } else {
                None
            };
            #[cfg(not(feature = "cloud"))]
            let reader = None;
            // `hive: true` says read this as one, which is the whole of what the row
            // promises — it is also what carries partition columns through, for a
            // directory the dispatch sends down the hive route. The cloud route returns
            // before the dispatch is reached.
            let directory = home::directory_dataset_url(&entry.path);
            return Some(self.home_open_directory_as(directory, true, lake, reader));
        }
        // A row nothing has looked at is looked at before it is opened, rather than
        // opened as whatever it turns out to be. `EntryKind::Unknown` is offered as
        // openable, so without this a lake root reached this way is read as one table:
        // #237 through the door #249 leaves open.
        let mut entry = entry;
        if entry.kind == discover::EntryKind::Unknown {
            if self.looking_could_block(&entry.path) {
                return Some(AppEvent::ClassifyThenOpen {
                    path: entry.path,
                    jump: false,
                });
            }
            if entry.path.is_dir() {
                entry.kind = discover::classify_directory(&entry.path);
            }
        }
        self.open_what_it_is(entry.path, entry.kind, false)
    }

    /// Whether finding out what a path is could sit on a mount that never answers.
    ///
    /// Two halves. An object-store or HTTP URL names something no mount is responsible
    /// for — what is behind it is the scan's business, and stat'ing it only ever asks the
    /// working directory about a file called `s3:` — and an ordinary local path answers at
    /// once, so making the user wait a round trip for it would be a delay bought with
    /// nothing.
    ///
    /// What is left is a path on a mount the home screen calls a network one, which is
    /// the case `is_remote_path` exists to name and the only one worth a worker.
    fn looking_could_block(&self, path: &Path) -> bool {
        // `cloud://<id>` is a place, not a path: `input_source` calls the unknown scheme
        // local and `is_remote_path` calls it remote, so without this a worker would be
        // sent to stat it and come back with "No such path".
        !home::is_cloud_place(path)
            && matches!(source::input_source(path), source::InputSource::Local(_))
            && (self.home.network_check)(path)
    }

    /// Do with a path whatever its kind calls for: browse into it, say it is a lake
    /// table, or open it.
    ///
    /// `jump` is a path typed at `~` rather than a row already listed, which starts a new
    /// browse so Esc comes back from there to the listing.
    fn open_what_it_is(
        &mut self,
        path: PathBuf,
        kind: discover::EntryKind,
        jump: bool,
    ) -> Option<AppEvent> {
        let go_inside = |app: &mut Self, path: PathBuf| {
            if jump {
                app.home_jump_into(path);
            } else {
                app.home_browse_into(path);
            }
        };
        if kind == discover::EntryKind::Directory {
            go_inside(self, path);
            return None;
        }
        // Nothing: the row is dimmed and its details pane says why.
        if kind == discover::EntryKind::Other {
            return None;
        }
        // A lake table's files are not its rows: the ones a delete or an update
        // tombstoned are still on disk, every rewritten version is here together, and
        // compaction leaves both sides in place. Going inside is what datui can honestly
        // do with one, and saying so is better than a silent wrong answer.
        if let Some(format) = kind.lake_name() {
            self.home.lake_here = Some((path.clone(), format));
            go_inside(self, path);
            return None;
        }
        // A cloud directory that is a dataset opens as one: its URL as a prefix, which is
        // what makes the open a scan of every file under it.
        let directory = matches!(
            kind,
            discover::EntryKind::Hive | discover::EntryKind::MultiFile
        );
        if directory && home::is_object_store_url(&path) {
            // A prefix, not a directory: the scan is what walks it.
            return Some(self.home_open_path(home::directory_dataset_url(&path), false));
        }
        // Said here, where the file was named, rather than after a download and a load
        // that could only end the same way. A row would be dimmed; a typed path has no
        // row, so the line says it.
        if kind == discover::EntryKind::File && discover::unreadable_by_name(&path) {
            self.home.status = Some(discover::NO_READER.to_string());
            return None;
        }
        Some(self.home_open_path(path, directory))
    }

    /// What `datui <path>` does with a directory: the same rule as `Enter` on its row.
    ///
    /// A directory used to be `Unsupported file type` unless `--hive` was passed, while
    /// pyarrow, Polars, pandas and Spark all open one. Naming a directory *is* the
    /// request to read it, so the three doors onto a path — the highlighted row, the `~`
    /// prompt and the command line — now answer the same: a hive root or a directory
    /// whose files are one table opens as one table, and a directory that is a place to
    /// look inside opens the home screen browsed into it, one keystroke from either file
    /// or union.
    ///
    /// The directory is looked into here rather than guessed at, because that is what the
    /// rule is: [`home::look_into`] is the same call the home screen's background pass
    /// makes, footers and all. On the command line it is on this thread, before the
    /// first frame, which is where the user is already waiting for the path they named.
    ///
    /// `--hive` is untouched. It names a glob or forces partition columns, and it is
    /// still the only way to say "read this as partitioned" about something whose
    /// layout does not say so itself.
    ///
    /// Returns the event to send, or `None` when the app is now at the home screen.
    pub fn open_the_path_named_on_the_command_line(
        &mut self,
        paths: Vec<PathBuf>,
        options: OpenOptions,
    ) -> Option<AppEvent> {
        // Several paths are a list of files to read together, and `--hive` is an answer
        // already given. Neither is a question about what one directory is.
        let single = (paths.len() == 1 && !options.hive).then(|| paths[0].clone());
        // A cloud directory is looked at too, by one page of its listing: what is in it
        // picks the reader, as it does for the `(all files)` row. Scanned blind, it was
        // read as Parquet whatever it held. A glob, a file name or `--format` already
        // says what to read.
        #[cfg(feature = "cloud")]
        if let Some(dir) = single.as_ref().filter(|p| {
            home::is_object_store_url(p)
                && options.format.is_none()
                && !p.to_string_lossy().contains('*')
                && !home::names_a_file(p)
        }) {
            return Some(AppEvent::LookThenOpenDirectory(dir.clone(), options));
        }
        let Some(dir) = single.filter(|p| p.is_dir()) else {
            return Some(AppEvent::Open(paths, options));
        };

        // Looking at a directory reads its footers, or the front of a spread of its
        // files. For a directory of large Parquet that is seconds — 4.6 of them on a real
        // one — and this runs before the first frame is drawn, so doing it here is a
        // blank terminal for the whole of it: no name, no spinner, no way out. It goes to
        // a worker, and the answer comes back as an event like every other read.
        Some(AppEvent::LookThenOpenDirectory(dir, options))
    }

    /// Act on what the look at a directory named on the command line found.
    ///
    /// The other half of [`Self::open_the_path_named_on_the_command_line`], which is
    /// where the reasoning for the rule itself is.
    fn open_the_directory_looked_at(
        &mut self,
        dir: PathBuf,
        kind: discover::EntryKind,
        holds: Option<&discover::Holds>,
        mut options: OpenOptions,
    ) -> Option<AppEvent> {
        #[cfg(feature = "cloud")]
        if home::is_object_store_url(&dir) {
            return self.open_the_cloud_directory_looked_at(dir, kind, holds, options);
        }
        let _ = holds;
        // No override for the user's reader settings here, and none needed: the look
        // read every file the way this open will, so `--no-header` and the skips have
        // already been accounted for by the rule rather than around it. Overriding
        // instead took three goes to get wrong in three different ways — it fired on
        // config values, it fired on directories with nothing readable in them, and it
        // fired on Parquet, which no CSV setting can affect.
        //
        // A lake table's files are not its rows, so the home screen is opened on it and
        // says why — the same sentence the row gives, because it is the same refusal.
        if let Some(format) = kind.lake_name() {
            self.enter_home();
            self.home.lake_here = Some((dir.clone(), format));
            self.home_jump_into(dir);
            return None;
        }
        // One table: read it. `hive` is what puts the open on the directory route, where
        // what the directory holds picks the reader.
        if matches!(
            kind,
            discover::EntryKind::Hive | discover::EntryKind::MultiFile
        ) {
            options.hive = true;
            self.set_loading_phase("Scanning input", 10);
            self.name_what_is_loading(dir.clone());
            self.busy = true;
            return Some(AppEvent::Open(vec![dir], options));
        }
        // A place to look inside. `datui .` is this, and so is a directory of separate
        // tables — where the `(all files)` row inside is the one keystroke that unions
        // them anyway.
        self.enter_home();
        self.home_jump_into(dir);
        None
    }

    /// As [`Self::open_the_directory_looked_at`], for a cloud directory: what `Enter`
    /// on its `(all files)` row does, or a browse into it when there is no data
    /// directly inside to read.
    #[cfg(feature = "cloud")]
    fn open_the_cloud_directory_looked_at(
        &mut self,
        dir: PathBuf,
        kind: discover::EntryKind,
        holds: Option<&discover::Holds>,
        options: OpenOptions,
    ) -> Option<AppEvent> {
        let open = |app: &mut Self, path: PathBuf, options: OpenOptions| {
            app.set_loading_phase("Scanning input", 10);
            app.name_what_is_loading(path.clone());
            app.busy = true;
            Some(AppEvent::Open(vec![path], options))
        };
        // The listing was refused. The open says why, in the words of whatever
        // refused it, which is what happened before anything looked.
        let Some(holds) = holds else {
            return open(self, dir, options);
        };
        if let Some(format) = kind.lake_name() {
            self.enter_home();
            self.home.lake_here = Some((dir.clone(), format));
            self.home_jump_into(dir);
            return None;
        }
        let directory = home::directory_dataset_url(&dir);
        if matches!(
            kind,
            discover::EntryKind::Hive | discover::EntryKind::MultiFile
        ) {
            let options = OpenOptions {
                hive: true,
                ..options
            };
            return open(self, directory, options);
        }
        if let Some((format, left_out)) = Self::cloud_prefix_format(holds) {
            let options = OpenOptions {
                hive: true,
                format: Some(format),
                left_out,
                ..options
            };
            return open(self, directory, options);
        }
        // Only directories, or nothing datui reads: somewhere to look inside, with
        // the reason when there is one.
        self.enter_home();
        self.home_jump_into(dir);
        self.home.status = Self::why_a_cloud_prefix_cannot_be_read(holds);
        None
    }

    /// Browse into `path` as a jump, from wherever the user was.
    ///
    /// Unlike `home_browse_into`, the browse *starts* here: Esc comes back from here to
    /// the listing rather than up through whatever the path happens to sit under.
    fn home_jump_into(&mut self, path: PathBuf) {
        self.home.browse_start = Some(path.clone());
        self.home.browsing = Some(path);
        self.home.status = None;
        self.home.search.reset();
        self.home.filter.clear();
        self.home.sync_search_section();
        self.home.selected = 0;
        self.home_refresh();
    }

    /// Load a path from the home screen.
    ///
    /// The recent entry is recorded by the `Open` handler, which every open goes
    /// through, so this does not record one itself.
    fn home_open_path(&mut self, path: PathBuf, hive: bool) -> AppEvent {
        self.home_open_directory(path, hive, None)
    }

    /// As [`Self::home_open_path`], and carrying whether the directory being read is a
    /// lake table whose plain files this read is, so the dataset can say so.
    fn home_open_directory(
        &mut self,
        path: PathBuf,
        hive: bool,
        lake: Option<&'static str>,
    ) -> AppEvent {
        self.home_open_directory_as(path, hive, lake, None)
    }

    /// As [`Self::home_open_directory`], naming the reader to use.
    ///
    /// For a prefix in an object store, where nothing downstream reads the listing: the
    /// cloud branches scan before the directory-format dispatch is reached, so the format
    /// the listing counted has to travel with the open or the scan falls back to
    /// Parquet, which is what it always did.
    fn home_open_directory_as(
        &mut self,
        path: PathBuf,
        hive: bool,
        lake: Option<&'static str>,
        reader: Option<(FileFormat, Vec<(FileFormat, usize)>)>,
    ) -> AppEvent {
        let (format, left_out) = match reader {
            Some((format, left_out)) => (Some(format), left_out),
            None => (None, Vec::new()),
        };
        self.load_from_home = true;
        // A directory of partitions is only meaningful read as one hive dataset. Told
        // rather than stat'ed: the caller already knows what this is, and on a share that
        // has gone away a `stat` here would freeze the thread reading the keys — the same
        // reason the size below is left to the `Open` handler.
        let options = OpenOptions {
            hive,
            read_as_plain_files_of: lake,
            format,
            left_out,
            ..OpenOptions::default()
        };
        self.input_mode = InputMode::Normal;
        self.set_loading_phase("Scanning input", 10);
        // A frame is drawn between this keypress and the `Open` that carries it out,
        // and it is the one the user is looking at when they press Enter — so it says
        // which file, not just that something is happening. `Open` fills in the size a
        // frame later; stat'ing here would put a possibly-dead mount on this thread.
        if let LoadingState::Loading { file_path, .. } = &mut self.loading_state {
            *file_path = Some(path.clone());
        }
        self.busy = true;
        AppEvent::Open(vec![path], options)
    }

    /// Key handling for the home screen.
    fn home_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
        // The line beside the prompt answers the last key, and this one replaces it: a
        // key with something to say sets it again below. Left up, "Forgot laps.parquet"
        // stayed until the next time the listing changed.
        self.home.status = None;

        // The home screen puts every plain character into the filter — `q` has to
        // type a `q`, or you could never search for "quarterly". Quitting is Ctrl+C,
        // handled before this is reached, and Esc once there is no context left to
        // back out of.
        if self.home.path_input_active {
            match event.code {
                KeyCode::Esc => {
                    self.home.path_input_active = false;
                    self.home.path_input.clear();
                    self.home.status = None;
                }
                KeyCode::Enter => {
                    let raw = self.home.path_input.trim().to_string();
                    if raw.is_empty() {
                        self.home.path_input_active = false;
                        return None;
                    }
                    let path = home::expand_user_path(&raw);
                    // A URL is not stat'ed: `exists` asks the working directory about a
                    // file called `gs:`. Its name decides, as it does for a recent — a
                    // file opens, and anything else in a bucket is browsed, where the
                    // listing says what is there.
                    if home::is_object_store_url(&path) || home::is_cloud_place(&path) {
                        self.home.path_input.clear();
                        self.home.path_input_active = false;
                        let kind = if home::names_a_file(&path) {
                            discover::EntryKind::File
                        } else {
                            discover::EntryKind::Directory
                        };
                        return self.open_what_it_is(path, kind, true);
                    }
                    // And an HTTP URL is one file.
                    if !matches!(source::input_source(&path), source::InputSource::Local(_)) {
                        self.home.path_input.clear();
                        self.home.path_input_active = false;
                        return self.open_what_it_is(path, discover::EntryKind::File, true);
                    }
                    // Whether it is there, whether it is a directory and what kind of one
                    // are three filesystem calls, and a typed path is exactly where a
                    // dead mount gets named. All three go to a worker when the mount is
                    // one that might not answer.
                    if self.looking_could_block(&path) {
                        self.home.path_input.clear();
                        self.home.path_input_active = false;
                        return Some(AppEvent::ClassifyThenOpen { path, jump: true });
                    }
                    // Before the prompt closes: a typo is worth fixing where it was
                    // typed, rather than retyping the whole path.
                    if !path.exists() {
                        self.home.status = Some(format!("No such path: {}", path.display()));
                        return None;
                    }
                    self.home.path_input.clear();
                    self.home.path_input_active = false;
                    let kind = if path.is_dir() {
                        discover::classify_directory(&path)
                    } else {
                        discover::EntryKind::File
                    };
                    return self.open_what_it_is(path, kind, true);
                }
                KeyCode::Backspace => {
                    self.home.path_input.pop();
                    self.home.status = None;
                }
                KeyCode::Char('u') if ctrl => self.home.path_input.clear(),
                // Completion reads a directory, which can block, so it is worked out
                // on a worker and applied when it comes back.
                KeyCode::Tab => self.request_path_completion(),
                KeyCode::Char(c) if !ctrl => {
                    self.home.path_input.push(c);
                    self.home.status = None;
                }
                _ => {}
            }
            return None;
        }

        // Every plain character types into the filter, so no letter or bracket is
        // a key here: typing "json" must not move the cursor on the "j". Navigation
        // is the arrows and the Ctrl chords, which cannot be part of a name.
        match event.code {
            KeyCode::Esc => return self.home_escape(),
            KeyCode::Enter => return self.home_open_selected(),
            // Section to section, past however many rows the current one holds.
            KeyCode::Down if ctrl => self.home.jump_section(1),
            KeyCode::Up if ctrl => self.home.jump_section(-1),
            KeyCode::Up => self.home.move_selection(-1),
            KeyCode::Down => self.home.move_selection(1),
            KeyCode::Char('n') if ctrl => self.home.move_selection(1),
            KeyCode::Char('p') if ctrl => self.home.move_selection(-1),
            // Left/right fold the section the cursor is in, wherever in it the cursor
            // happens to be — so collapsing does not require first finding the header.
            // Tab cycles the sort. Every plain key goes into the filter, so an
            // ordinary letter is not available for this.
            KeyCode::Tab => {
                self.home.sort = self.home.sort.next();
                self.home.select_first_entry();
            }
            KeyCode::Left => self.home_collapse(true),
            KeyCode::Right => match self.selected_directory_to_enter() {
                // Into a directory that opens as one dataset rather than opening it, to
                // reach one partition or one file. This clears the filter, as browsing
                // anywhere does.
                Some(directory) => {
                    // The heading Enter leaves, for the same reason: this is the door
                    // the control bar advertises on a lake row, and arriving inside one
                    // with no explanation is the silent wrong answer #237 is about.
                    if let Some(format) = self
                        .home
                        .selected_entry()
                        .and_then(|entry| entry.kind.lake_name())
                    {
                        self.home.lake_here = Some((directory.clone(), format));
                    }
                    self.home_browse_into(directory);
                }
                None => self.home_collapse(false),
            },
            // A screenful, matching the table; the renderer keeps view_height current.
            KeyCode::PageUp => {
                let page = self.home.view_height.max(1) as isize;
                self.home.page_selection(-page);
            }
            KeyCode::PageDown => {
                let page = self.home.view_height.max(1) as isize;
                self.home.page_selection(page);
            }
            KeyCode::Home => self.home.page_selection(isize::MIN),
            KeyCode::End => self.home.page_selection(isize::MAX),
            KeyCode::Char('u') if ctrl => {
                self.home.filter.clear();
                self.home.sync_search_section();
                self.home.select_first_entry();
            }
            KeyCode::Char('r') if ctrl => self.home_reload(),
            // A browser's bookmark key: keep this place on the home screen, or stop.
            KeyCode::Char('d') if ctrl => self.home_toggle_remembered(),
            KeyCode::Char('a') if ctrl => {
                let on = self.home.selected_key();
                self.home.hide_unreadable = !self.home.hide_unreadable;
                self.flash_note(if self.home.hide_unreadable {
                    "Hiding files datui can't open".to_string()
                } else {
                    "Showing files datui can't open".to_string()
                });
                // The same row where it is still there; the cursor stays put otherwise.
                self.home.reselect(on);
            }
            KeyCode::Backspace => {
                if self.home.filter.is_empty() {
                    self.home_ascend();
                } else {
                    self.home.filter.pop();
                    self.home.sync_search_section();
                    self.home.select_first_entry();
                }
            }
            // Forget the highlighted entry. Only meaningful in Recent — elsewhere the
            // row is a real directory listing, and datui does not delete files.
            // Shift+Delete forgets the lot. It sits next to the key that forgets
            // one, so it asks first — an accidental press should not silently throw
            // away every place the user has been.
            KeyCode::Delete if event.modifiers.contains(KeyModifiers::SHIFT) => {
                let count = self.cache.load_recents().len();
                if count == 0 {
                    self.home.status = Some("Nothing to forget".into());
                } else {
                    self.pending_clear_recents = true;
                    self.confirmation_modal
                        .show(format!("Forget all {count} recently opened datasets?"));
                }
            }
            KeyCode::Delete => self.home_forget_selected(),
            KeyCode::Char('~') if self.home.filter.is_empty() => {
                self.home.path_input_active = true;
                self.home.status = None;
            }
            // The one printable that is a key, and only before typing starts: a
            // filter beginning with a literal `?` matches nothing anyway, and this
            // is where a new user asks for the keys. F1 opens help mid-filter.
            KeyCode::Char('?') if self.home.filter.is_empty() && !ctrl => {
                self.open_help_overlay();
            }
            // Space before typing starts folds a header, as Enter does, and is otherwise
            // nothing: a filter of one space is invisible at the prompt and matched every
            // name with a space in it, below the working directory too.
            KeyCode::Char(' ') if self.home.filter.is_empty() && !ctrl => {
                if self.home.selection_is_header() {
                    self.home_toggle_fold();
                }
            }
            KeyCode::Char(c) if !ctrl => {
                self.home.filter.push(c);
                // Typing is what asks for the recursive search. Starting it here and
                // not on open means the walk is only ever paid for by someone who is
                // actually looking for something.
                self.spawn_home_search();
                self.home.sync_search_section();
                self.home.select_first_entry();
            }
            _ => {}
        }
        None
    }

    /// Get a color from the theme by name
    fn color(&self, name: &str) -> Color {
        self.theme.get(name)
    }

    /// The export format to offer by default for a dataset opened from `path`.
    ///
    /// An explicit `--format` wins, then the extension. A compressed CSV keeps its CSV
    /// identity: `sales.csv.gz` has extension `gz`, and the `.csv` that matters is in
    /// the stem, so reading the extension alone offered no default at all.
    fn export_format_for(path: &Path, options: &OpenOptions) -> Option<ExportFormat> {
        options
            .format
            .or_else(|| FileFormat::from_path(path))
            .and_then(file_format_to_export_format)
            .or_else(|| {
                path.file_stem()
                    .and_then(|s| s.to_str())
                    .filter(|s| s.ends_with(".csv"))
                    .map(|_| ExportFormat::Csv)
            })
    }

    /// Read a compressed CSV into a table state.
    ///
    /// This is the one input datui cannot scan lazily: the file has to be
    /// decompressed and parsed before anything can be shown, which for a large export
    /// is minutes. It takes no `&self` so it can run on a background thread.
    fn decompressed_csv_state(path: &Path, options: &OpenOptions) -> Result<DataTableState> {
        DataTableState::from_csv(path, options)
    }

    /// Polars' view of one source's S3 settings, for `scan_parquet`.
    #[cfg(feature = "cloud")]
    fn build_s3_cloud_options(settings: &crate::cloud_sources::S3Settings) -> CloudOptions {
        let settings = settings.clone();
        let virtual_hosted = (settings.endpoint.is_some() || settings.virtual_hosted.is_some())
            .then(|| settings.virtual_hosted_style().to_string());
        let configs: Vec<(AmazonS3ConfigKey, String)> = [
            (AmazonS3ConfigKey::Endpoint, settings.endpoint),
            (AmazonS3ConfigKey::AccessKeyId, settings.access_key_id),
            (
                AmazonS3ConfigKey::SecretAccessKey,
                settings.secret_access_key,
            ),
            (AmazonS3ConfigKey::Token, settings.session_token),
            (AmazonS3ConfigKey::Region, settings.region),
            (AmazonS3ConfigKey::VirtualHostedStyleRequest, virtual_hosted),
            (
                AmazonS3ConfigKey::SkipSignature,
                settings.skip_signature.then(|| "true".to_string()),
            ),
        ]
        .into_iter()
        .filter_map(|(key, value)| value.map(|v| (key, v)))
        .collect();
        let opts = CloudOptions::default();
        if configs.is_empty() {
            opts
        } else {
            opts.with_aws(configs)
        }
    }

    /// The bucket and key of an `s3://bucket/key` or `gs://bucket/key` URL. The key
    /// is empty for a bucket root.
    #[cfg(feature = "cloud")]
    fn cloud_bucket_and_key(url: &str) -> Result<(String, String)> {
        if let Some((_, container, key)) = source::azure_parts(url) {
            return Ok((container, key.trim_matches('/').to_string()));
        }
        crate::cloud_browse::split_bucket_url(url)
            .map(|(_, bucket, key)| (bucket, key))
            .ok_or_else(|| {
                color_eyre::eyre::eyre!("URL must be s3://bucket/key or gs://bucket/key")
            })
    }

    /// The store Polars itself will scan `url` through, from its cache keyed on the
    /// bucket and `options`, so the footer read, the size probe and a download share
    /// one credential chain, TLS client and connection pool with the scan instead of
    /// each building a store of their own.
    #[cfg(feature = "cloud")]
    fn polars_object_store(
        url: &str,
        options: &CloudOptions,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Arc<dyn object_store::ObjectStore>> {
        let url = url.to_string();
        let options = options.clone();
        wait_on_runtime(runtime, async move {
            let (_, store) = polars::io::cloud::build_object_store(
                PlRefPath::new(url.as_str()),
                Some(&options),
                false,
            )
            .await?;
            polars::prelude::PolarsResult::Ok(store.to_dyn_object_store().await.into_owned())
        })
        .ok_or_else(|| color_eyre::eyre::eyre!("cancelled"))?
        .map_err(|e| color_eyre::eyre::eyre!("Object store config failed: {}", e))
    }

    /// Human-readable byte size, for the download confirmation and the load's progress.
    fn format_bytes(n: u64) -> String {
        const KB: u64 = 1024;
        const MB: u64 = KB * 1024;
        const GB: u64 = MB * 1024;
        const TB: u64 = GB * 1024;
        if n >= TB {
            format!("{:.2} TB", n as f64 / TB as f64)
        } else if n >= GB {
            format!("{:.2} GB", n as f64 / GB as f64)
        } else if n >= MB {
            format!("{:.2} MB", n as f64 / MB as f64)
        } else if n >= KB {
            format!("{:.2} KB", n as f64 / KB as f64)
        } else {
            format!("{} bytes", n)
        }
    }

    /// Build an HTTP agent with a total time budget.
    ///
    /// ureq 3 moved timeouts off the request and onto agent configuration, so
    /// every request has to come from an agent to be bounded at all. Leaving a
    /// request unbounded would mean a remote that accepts a connection and then
    /// dribbles bytes forever hangs the whole TUI, and the user's only way out
    /// is to kill the process.
    ///
    /// `timeout_global` covers the entire exchange rather than individual
    /// socket operations, which is the property that matters here: a server
    /// that sends one byte every 29 seconds defeats a per-read timeout but not
    /// this one.
    #[cfg(feature = "http")]
    fn http_agent(total: std::time::Duration) -> ureq::Agent {
        ureq::Agent::config_builder()
            .timeout_global(Some(total))
            .build()
            .into()
    }

    #[cfg(feature = "http")]
    fn fetch_remote_size_http(url: &str) -> Result<Option<u64>> {
        let agent = Self::http_agent(std::time::Duration::from_secs(15));
        match agent.head(url).call() {
            Ok(r) => Ok(r
                .headers()
                .get("Content-Length")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())),
            Err(_) => Ok(None),
        }
    }

    /// The size of one S3 or GCS object, from a HEAD through the shared store.
    #[cfg(feature = "cloud")]
    fn fetch_remote_size_cloud(
        url: &str,
        cloud: &crate::config::CloudConfig,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Option<u64>> {
        use object_store::ObjectStoreExt;

        let (_bucket, key) = Self::cloud_bucket_and_key(url)?;
        if key.is_empty() {
            return Ok(None);
        }
        let (_, _, store) = Self::cloud_store_for(Path::new(url), cloud, runtime)?;
        let path = crate::cloud_browse::object_path(&key);
        let head = wait_on_runtime(runtime, async move { store.head(&path).await });
        Ok(head.and_then(|r| r.ok()).map(|meta| meta.size))
    }

    #[cfg(feature = "http")]
    fn download_http_to_temp(
        url: &str,
        temp_dir: Option<&Path>,
        extension: Option<&str>,
    ) -> Result<PathBuf> {
        let dir = temp_dir
            .map(Path::to_path_buf)
            .unwrap_or_else(std::env::temp_dir);
        let suffix = extension
            .map(|e| format!(".{e}"))
            .unwrap_or_else(|| ".tmp".to_string());
        let mut temp = tempfile::Builder::new()
            .suffix(&suffix)
            .tempfile_in(&dir)
            .map_err(|_| color_eyre::eyre::eyre!("Could not create a temporary file."))?;
        let agent = Self::http_agent(std::time::Duration::from_secs(300));
        let mut response = agent.get(url).call().map_err(|e| {
            color_eyre::eyre::eyre!("Download failed. Check the URL and your connection: {}", e)
        })?;
        let status = response.status();
        if status.is_client_error() || status.is_server_error() {
            return Err(color_eyre::eyre::eyre!(
                "Server returned {} {}. Check the URL.",
                status.as_u16(),
                status.canonical_reason().unwrap_or("Unknown")
            ));
        }
        std::io::copy(&mut response.body_mut().as_reader(), &mut temp)
            .map_err(|_| color_eyre::eyre::eyre!("Download failed while saving the file."))?;
        let (_file, path) = temp
            .keep()
            .map_err(|_| color_eyre::eyre::eyre!("Could not save the downloaded file."))?;
        Ok(path)
    }

    /// Download one S3 or GCS object to a temporary file, named for the user by its
    /// scheme in any error.
    #[cfg(feature = "cloud")]
    fn download_cloud_to_temp(
        url: &str,
        cloud: &crate::config::CloudConfig,
        options: &OpenOptions,
        runtime: &tokio::runtime::Handle,
    ) -> Result<PathBuf> {
        use object_store::ObjectStoreExt;

        let (label, example) = match source::input_source(Path::new(url)) {
            source::InputSource::Gcs(_) => ("GCS", "gs://bucket/path/file.csv"),
            source::InputSource::Azure(_) => (
                "Azure",
                "abfss://container@account.dfs.core.windows.net/path/file.csv",
            ),
            _ => ("S3", "s3://bucket/path/file.csv"),
        };
        let ext = source::download_suffix(url);
        let (_bucket, key) = Self::cloud_bucket_and_key(url)?;
        if key.is_empty() {
            return Err(color_eyre::eyre::eyre!(
                "{label} URL must point to an object (e.g. {example})"
            ));
        }
        let (_, _, store) = Self::cloud_store_for(Path::new(url), cloud, runtime)?;

        let path = crate::cloud_browse::object_path(&key);
        let bytes = wait_on_runtime(runtime, async move {
            let get_result = store.get(&path).await.map_err(|e| {
                color_eyre::eyre::eyre!(
                    "Could not read from {label}. Check credentials and URL: {}",
                    e
                )
            })?;
            get_result
                .bytes()
                .await
                .map_err(|e| color_eyre::eyre::eyre!("Could not read {label} object body: {}", e))
        })
        .ok_or_else(|| color_eyre::eyre::eyre!("{label} download was cancelled."))??;

        let dir = options.temp_dir.clone().unwrap_or_else(std::env::temp_dir);
        let suffix = ext
            .as_ref()
            .map(|e| format!(".{e}"))
            .unwrap_or_else(|| ".tmp".to_string());
        let mut temp = tempfile::Builder::new()
            .suffix(&suffix)
            .tempfile_in(&dir)
            .map_err(|_| color_eyre::eyre::eyre!("Could not create a temporary file."))?;
        std::io::copy(&mut std::io::Cursor::new(bytes.as_ref()), &mut temp)
            .map_err(|_| color_eyre::eyre::eyre!("Could not write downloaded file."))?;
        let (_file, path_buf) = temp
            .keep()
            .map_err(|_| color_eyre::eyre::eyre!("Could not save the downloaded file."))?;
        Ok(path_buf)
    }

    /// Build LazyFrame from paths for phased loading (non-compressed only). Caller must not use for compressed CSV.
    /// Ask the store how big a remote file is, off the event thread.
    ///
    /// The answer only feeds a confirmation message, but getting it means a HEAD
    /// request: fifteen seconds of timeout for HTTP, unbounded for S3 and GCS. Doing
    /// that inline froze the UI, and froze it precisely where the user is most likely
    /// to want out.
    #[cfg(any(feature = "http", feature = "cloud"))]
    fn spawn_remote_size_probe(&mut self, pending: PendingDownload) -> Option<AppEvent> {
        #[cfg(feature = "cloud")]
        let (cloud, runtime) = (self.app_config.cloud.clone(), self.runtime.clone());
        self.spawn_bg("Checking size...", move |task_gen, tx| {
            let size = match &pending {
                #[cfg(feature = "http")]
                PendingDownload::Http { url, .. } => {
                    Self::fetch_remote_size_http(url).unwrap_or(None)
                }
                #[cfg(feature = "cloud")]
                PendingDownload::S3 { url, .. }
                | PendingDownload::Gcs { url, .. }
                | PendingDownload::Azure { url, .. } => {
                    Self::fetch_remote_size_cloud(url, &cloud, &runtime).unwrap_or(None)
                }
            };
            let _ = tx.send(AppEvent::BackgroundRemoteSizeReady {
                generation: task_gen,
                pending: Box::new(pending.with_size(size)),
            });
        });
        None
    }

    /// What the user is being asked to agree to before a remote file is downloaded.
    #[cfg(any(feature = "http", feature = "cloud"))]
    fn download_confirmation_message(pending: &PendingDownload) -> String {
        let (url, size, options) = pending.parts();
        let size_str = size
            .map(Self::format_bytes)
            .unwrap_or_else(|| "unknown".to_string());
        let dest_dir = options
            .temp_dir
            .as_deref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| std::env::temp_dir().display().to_string());
        format!(
            "URL: {url}\nFile size: {size_str}\nDestination: {dest_dir} (temporary file)\n\nContinue with download?"
        )
    }

    /// Run the LazyFrame scan for `paths` on a background thread.
    ///
    /// Scanning is where the wall-clock time goes — CSV schema inference, and hive
    /// directories with many files — so doing it on the event thread freezes the UI
    /// for its whole duration: no repaint, no throbber, no way out. Both CSV entry
    /// points funnel through here.
    fn spawn_scan(
        &mut self,
        status: &str,
        paths: Vec<PathBuf>,
        options: OpenOptions,
    ) -> Option<AppEvent> {
        self.spawn_scan_as(status, paths, options, None)
    }

    /// As [`App::spawn_scan`], but reporting `display_path` as the dataset's identity.
    ///
    /// A downloaded remote file is scanned from a temp path the user never typed and
    /// would not recognise; the URL they did type is what belongs on screen.
    fn spawn_scan_as(
        &mut self,
        status: &str,
        paths: Vec<PathBuf>,
        options: OpenOptions,
        display_path: Option<PathBuf>,
    ) -> Option<AppEvent> {
        let cloud = self.app_config.cloud.clone();
        let path_for_event = display_path.or_else(|| paths.first().cloned());
        let slot = self.pending_lazyframe_result.clone();
        self.spawn_bg(status, move |task_gen, tx| {
            // What the read passed over rides back with the options it was asked for, so
            // the dataset can say what it left out. Seeded with what the caller already
            // knows and overwritten by what the read finds: a directory on disk is the
            // read's own answer, because it is the pass that decides, while for a prefix
            // in an object store Polars does the listing and never sees the other formats
            // — there the home screen's listing is the only witness.
            let mut report = ReadReport {
                left_out: options.left_out.clone(),
                files_disagree: options.files_disagree,
                format: None,
            };
            match Self::build_lazyframe_from_paths_with(&cloud, &paths, &options, &mut report) {
                Ok(lf) => {
                    let options = OpenOptions {
                        left_out: report.left_out,
                        files_disagree: report.files_disagree,
                        format: report.format.or(options.format),
                        ..options
                    };
                    let mut guard = slot.lock().unwrap_or_else(|e| e.into_inner());
                    // A newer scan already landed; this result is obsolete.
                    let dominated = guard.as_ref().is_some_and(|(g, _)| *g > task_gen);
                    if !dominated {
                        *guard = Some((task_gen, lf));
                    }
                    drop(guard);
                    let _ = tx.send(AppEvent::BackgroundLazyFrameReady {
                        generation: task_gen,
                        path: path_for_event,
                        options,
                    });
                }
                Err(e) => {
                    let _ = tx.send(AppEvent::BackgroundError {
                        generation: task_gen,
                        message: crate::error_display::user_message_from_report(
                            &e,
                            paths.first().map(|p| p.as_path()),
                        ),
                    });
                }
            }
        });
        None
    }

    /// Take the offer on the note the cursor is on: read its column as text.
    ///
    /// Only a note that carries the offer has one, and the offer is taken off a note
    /// datui could not act on, so the `Ok(false)` arms here are for a note that has
    /// gone stale under the cursor rather than for anything to tell the user about. A
    /// failure is the scan's, and is shown the way any other failed read is.
    fn read_the_selected_note_s_column_as_text(&mut self) {
        let Some(state) = self.data_table_state.as_mut() else {
            return;
        };
        let notes = state.notes();
        let Some(column) = notes
            .get(self.info_modal.notes_selected_index)
            .and_then(|note| note.read_as_text.clone())
        else {
            return;
        };
        match state.read_column_as_text(&column) {
            Ok(true) => {
                // The note that offered this is gone and the list is shorter, so the
                // cursor would otherwise sit past the end. Kept as near to where the
                // user left it as the shorter list allows, rather than thrown to the
                // top: one or two notes went, not all of them.
                let notes = state.notes().len();
                self.info_modal.notes_selected_index = self
                    .info_modal
                    .notes_selected_index
                    .min(notes.saturating_sub(1));
                self.info_modal.notes_scroll_offset = 0;
            }
            Ok(false) => {}
            Err(error) => state.error = Some(error),
        }
    }

    fn hoist_partition_columns(
        lf: LazyFrame,
        schema: &Schema,
        partition_columns: &[String],
        drifts: bool,
    ) -> LazyFrame {
        hoist_partition_columns(lf, schema, partition_columns, drifts)
    }
}

/// Put hive partition columns first, ahead of the file's own columns. `drifts` keeps
/// the scan's hidden drift column, which the select would otherwise drop.
///
/// A free function rather than a method: rebuilding the scan to read a column as text
/// has to put the columns back the same way, and it happens on the table's state
/// rather than on the app.
pub(crate) fn hoist_partition_columns(
    lf: LazyFrame,
    schema: &Schema,
    partition_columns: &[String],
    drifts: bool,
) -> LazyFrame {
    if partition_columns.is_empty() {
        return lf;
    }
    let mut exprs: Vec<_> = partition_columns
        .iter()
        .map(|s| col(s.as_str()))
        .chain(
            schema
                .iter_names()
                .map(|s| s.to_string())
                .filter(|c| !partition_columns.contains(c))
                .map(|s| col(s.as_str())),
        )
        .collect();
    if drifts {
        exprs.push(col(crate::schema_union::DRIFT_COLUMN));
    }
    lf.select(exprs)
}

/// What a pass behind a staged open reported, and which dataset it was reading for.
/// `None` where the footers are: a pass that could not read them says so, so the
/// dataset stops waiting.
type FootersReported = Option<(u64, Option<crate::widgets::datatable::FootersFound>)>;

/// A cloud dataset as some set of its footers describes it.
///
/// The open builds one from the two ends of the listing and the pass behind it builds
/// another from every footer; what tells them apart is only how much they know.
#[cfg(feature = "cloud")]
struct CloudDataset {
    dataset: crate::schema_union::DatasetSchema,
    /// Each file's rows, or empty when they are not all known — the same condition
    /// under which the scan declines to number its rows.
    file_rows: Vec<usize>,
    urls: Vec<String>,
    /// Each file's row groups, or empty unless every footer was read and parsed.
    row_groups: Vec<Vec<usize>>,
    scan: crate::widgets::datatable::FileScan,
    partition_columns: Vec<String>,
}
impl App {
    /// Schema for a local directory of Parquet files: every column any of them has, from
    /// their footers, instead of `collect_schema()` over the whole set or one file's
    /// columns standing in for all.
    ///
    /// Reading a local footer is a seek and a small read, so this is cheap even for
    /// thousands of files, and it is what makes a column a vendor added for a month
    /// visible. `None` when the path is not that shape, or when nothing could be read —
    /// either way the caller falls back to the general scan, which reports the error
    /// properly if there is one.
    fn schema_state_from_local_hive(
        path: Option<&Path>,
        options: &OpenOptions,
        progress: &crate::schema_union::FooterProgress,
        meter: &crate::measurements::Meter,
    ) -> Option<DataTableState> {
        if !options.single_spine_schema {
            return None;
        }
        let p = path.filter(|p| p.is_dir() && options.hive)?;
        let (files, read, footers, skipped) =
            DataTableState::footers_of_parquet_dir_reporting(p, progress, meter);
        let first = files.first()?;
        let partition_columns = DataTableState::discover_hive_partition_columns(p);
        let values = DataTableState::hive_partition_values(p, first);
        let mut dataset = crate::schema_union::union_sampled(files.len(), &read, &footers);
        if dataset.schema.is_empty() {
            return None;
        }
        dataset.schema = Arc::new(crate::schema_union::with_partition_columns(
            &dataset.schema,
            &partition_columns,
            &values,
        ));
        let paths: Vec<String> = files
            .iter()
            .map(|f| f.to_string_lossy().into_owned())
            .collect();
        // Numbering rows needs every file's row count; a sampled dataset has not read
        // them all, so it forgoes the distinction rather than guessing at it.
        let file_rows: Vec<usize> = if read.len() == files.len() {
            footers
                .iter()
                .map(|f| f.as_ref().map(|f| f.rows))
                .collect::<Option<Vec<_>>>()
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        let drift = crate::schema_union::ScanDrift::new(&paths, &dataset, &file_rows);
        let schema = dataset.schema.clone();
        // Over the files that will open. `drift` is keyed by path, so a scan of fewer
        // of them still knows what each one holds.
        let readable = crate::schema_union::readable_paths(&paths, &dataset.unreadable);
        // Belt and braces: a dataset with nothing readable has an empty schema and has
        // already been handed back above.
        if readable.is_empty() {
            return None;
        }
        let lf =
            crate::schema_union::lenient_scan(&readable, schema.clone(), None, drift.as_ref(), &[])
                .ok()?;
        let lf = Self::hoist_partition_columns(lf, &schema, &partition_columns, drift.is_some());
        let mut state =
            DataTableState::from_schema_and_lazyframe(schema, lf, options, Some(partition_columns))
                .ok()?;
        state.set_dataset_schema(
            dataset
                .with_partition_layouts(&p.to_string_lossy(), &paths)
                .with_skipped(skipped),
            &file_rows,
            &paths,
        );
        Some(state)
    }

    /// The same one-file trick against an object store. This is the route that used to
    /// block the UI thread on a network round trip.
    #[cfg(feature = "cloud")]
    fn schema_state_from_cloud_hive(
        path: Option<&Path>,
        options: &OpenOptions,
        cloud: &crate::config::CloudConfig,
        runtime: &tokio::runtime::Handle,
        report: &crate::measurements::OpenReport,
    ) -> Option<DataTableState> {
        if !options.single_spine_schema {
            return None;
        }
        // Unlike the local path this does not require --hive: a directory or glob URL
        // is already a hive scan by shape.
        let p = path.filter(|p| {
            let s = p.as_os_str().to_string_lossy();
            home::is_object_store_url(p) && (options.hive || source::is_prefix_or_glob(&s))
        })?;

        let (full, cloud_opts, store) = Self::cloud_store_for(p, cloud, runtime).ok()?;
        let (_bucket, key) = Self::cloud_bucket_and_key(&full).ok()?;
        Self::schema_state_from_cloud_hive_with(
            full, key, store, cloud_opts, options, runtime, report,
        )
    }

    /// The same, against a store already built.
    ///
    /// Split out so a test can hand it an in-memory store and cover the choice between
    /// the two routes below — including that each is given the counter it was called
    /// with, rather than one of its own.
    #[cfg(feature = "cloud")]
    fn schema_state_from_cloud_hive_with(
        full: String,
        key: String,
        store: Arc<dyn object_store::ObjectStore>,
        cloud_opts: CloudOptions,
        options: &OpenOptions,
        runtime: &tokio::runtime::Handle,
        report: &crate::measurements::OpenReport,
    ) -> Option<DataTableState> {
        // Every file listed once, and the scan, the schema and the count all work from
        // that list — for a glob as much as for a prefix. datui expands the glob
        // itself: it lists the literal part of the key and matches the rest, so a glob
        // is an ordinary list of files by the time anything else sees it, and gets the
        // schema union, the row count, the notes and the measurements that a prefix
        // gets.
        //
        // The star cannot be handed to the object store. A listing prefix is a literal
        // string, so `data/*.parquet` matches nothing and the open falls through to a
        // whole-dataset scan with none of the above — which is what used to happen, for
        // every glob, silently (#228).
        let pattern = full.contains('*').then(|| {
            globset::GlobBuilder::new(&key)
                .literal_separator(true)
                .build()
                .map(|g| g.compile_matcher())
        });
        let pattern = match pattern {
            // A pattern datui cannot read is not one it should guess at.
            Some(Err(_)) => return None,
            Some(Ok(matcher)) => Some(matcher),
            None => None,
        };
        let listed = cloud_hive::prefix_of_glob(&key).to_string();
        Self::schema_state_from_cloud_files(
            CloudTarget {
                full: &full,
                key: listed,
                pattern: pattern.as_ref(),
            },
            store,
            cloud_opts,
            options,
            runtime,
            report,
        )
    }

    /// A cloud prefix of Parquet files as one dataset, from a single listing of it.
    ///
    /// The files are scanned by name, so Polars does not list the prefix again, and
    /// leniently (see `cloud_hive::lenient_scan`), since files written years apart
    /// differ. The state keeps the list, so the count reads footers rather than data
    /// and a buffer reads only the files holding its rows (see `RemoteFiles`).
    #[cfg(feature = "cloud")]
    fn schema_state_from_cloud_files(
        target: CloudTarget<'_>,
        store: Arc<dyn object_store::ObjectStore>,
        cloud_opts: CloudOptions,
        options: &OpenOptions,
        runtime: &tokio::runtime::Handle,
        report: &crate::measurements::OpenReport,
    ) -> Option<DataTableState> {
        let CloudTarget { full, key, pattern } = target;
        // Kept before the listing takes ownership of it: this is the prefix that was
        // listed, and the notes measure every file's path against it.
        let root = cloud_hive::url_of_key(full, &key).unwrap_or_else(|| full.to_string());
        let (files, skipped) = {
            let store = store.clone();
            // The listing is one `list` whose pages object_store turns over itself, so
            // this brackets the whole of it: the first request to the last page. The
            // request count is not datui's to give — the paging happens inside the
            // store — so the listing reports a time and the data files it found, and
            // leaves requests and bytes to the footer pass, which does issue its own.
            // The count is after the filtering: what is reported is the dataset's
            // files, not every object under the prefix.
            let listing_began = std::time::Instant::now();
            let pattern = pattern.cloned();
            let (files, skipped) = wait_on_runtime(runtime, async move {
                cloud_hive::list_dataset_files(&store, &key, pattern.as_ref()).await
            })?
            .ok()?;
            report
                .meter
                .listed(listing_began.elapsed(), Some(files.len()), false);
            (Arc::new(files), skipped)
        };
        // Past one wave of concurrent reads the footers stop being free: the two ends
        // open the dataset and the rest are read behind it, joining when they land. Up
        // to a wave they cost one round trip either way, so the dataset opens whole —
        // rows numbered, absent cells marked, notes complete.
        // What the listing says this dataset is now. Taking it costs nothing — the
        // listing has already happened, and it is the only thing that has to — and it
        // is what decides whether the footers can be skipped entirely.
        let fingerprint = crate::cache::DatasetShape::fingerprint_of(
            files
                .iter()
                .map(|f| (f.key.as_str(), f.size, f.stamp, f.etag.as_deref())),
        );
        let remembered = report
            .remembered
            .as_ref()
            .and_then(|cache| cache.dataset_shape(full, &fingerprint))
            // No length check: the fingerprint leads with the file count, so a listing
            // of a different size cannot match one in the first place.
            .and_then(|shape| cloud_hive::footers_from_cache(&shape.files, &shape.schemas));

        let staged = remembered.is_none() && files.len() > cloud_hive::FOOTERS_AT_ONCE;
        let read = if remembered.is_some() {
            // Every file, because the cache holds every file: a remembered dataset
            // opens whole, with its rows numbered and its notes complete, however large
            // it is. That is the point of remembering it.
            (0..files.len()).collect()
        } else if staged {
            crate::schema_union::ends_of(files.len())
        } else {
            crate::schema_union::footers_to_read(files.len())
        };
        let footers = match remembered {
            Some(cached) => cached,
            None => Self::cloud_footers(
                store.clone(),
                files.clone(),
                read.clone(),
                runtime,
                report.progress.clone(),
                report.meter.clone(),
            )?,
        };
        Self::remember_dataset_shape(
            report.remembered.as_ref(),
            full,
            &fingerprint,
            &read,
            &files,
            &footers,
        );
        let opened =
            Self::cloud_dataset_from_footers(full, &root, &files, &read, &footers, &cloud_opts)?;
        let CloudDataset {
            dataset,
            file_rows,
            urls,
            row_groups,
            scan,
            partition_columns,
        } = opened;
        let schema = dataset.schema.clone();
        // The objects that will open. One whose footer would not read is one Polars
        // cannot read either, and left in the scan it takes the whole prefix down with
        // it on the first page.
        //
        // A staged open can only leave out what it has read: two footers, so an object
        // that will not parse anywhere but the two ends is in this scan and the first
        // page fails on it. That is a window, not a lost guarantee — the pass behind
        // the open finds it and the join swaps in a scan without it — but for a directory
        // with a file mid-write, a prefix over sixty-four objects shows an error where
        // a smaller one shows rows.
        let readable = crate::schema_union::readable_paths(&urls, &dataset.unreadable);
        // Everything downstream describes the same list or none of it. The counter
        // returns one entry per object it is given and `set_file_row_groups` wants one
        // per url, so a counter over the full listing beside a shorter url list is not
        // a wrong count, it is no count at all: the lengths disagree, the answer is
        // dropped without a word, and the dataset spends the rest of the session
        // re-counting itself and never reaching an end to jump to.
        let counted: Vec<cloud_hive::DatasetFile> = files
            .iter()
            .enumerate()
            // Searched rather than scanned, for the same reason `readable_paths` does:
            // a prefix can be hundreds of thousands of objects.
            .filter(|(index, _)| dataset.unreadable.binary_search(index).is_err())
            .map(|(_, file)| file.clone())
            .collect();
        // Belt and braces, both of them: a prefix with nothing readable has no schema
        // and was handed back above, and the two lists are filtered from the same
        // indices so they cannot come out different lengths. Kept because the cost of
        // the invariant quietly breaking is a dataset that counts itself forever and
        // never finds its end, which is not a thing to leave to a comment.
        if readable.is_empty() || readable.len() != counted.len() {
            return None;
        }
        let count: crate::widgets::datatable::FileCounter = {
            let (runtime, counted, store) = (runtime.clone(), Arc::new(counted), store.clone());
            // The same meter again: this counts by re-reading every footer, so its
            // requests are footer requests and belong in the same tally.
            let meter = report.meter.clone();
            Arc::new(move || {
                let (store, counted, meter) = (store.clone(), counted.clone(), meter.clone());
                wait_on_runtime(&runtime, async move {
                    cloud_hive::row_groups_of_files(&store, &counted, &meter).await
                })
                .ok_or_else(|| "cancelled".to_string())?
                .map_err(|e| e.to_string())
            })
        };
        let lf = scan(&readable, &[]).ok()?;
        let mut state =
            DataTableState::from_schema_and_lazyframe(schema, lf, options, Some(partition_columns))
                .ok()?;
        state.set_remote_files(crate::widgets::datatable::RemoteFiles {
            urls: Arc::new(readable.into_owned()),
            scan,
            count,
            offsets: None,
        });
        // The footers just read hold the count too, so the dataset opens counted — but
        // `cloud_dataset_from_footers` gives row groups only when every file was read
        // and every footer parsed. A footer sampled past or failed would count as no
        // rows, which both undercounts the dataset and puts that file's rows out of
        // reach of a windowed scan; leaving the count to `RemoteFiles::count` means it
        // is retried instead.
        if !row_groups.is_empty() {
            state.set_file_row_groups(&row_groups);
        }
        state.set_dataset_schema(dataset.with_skipped(skipped), &file_rows, &urls);
        if staged {
            // Everything the pass behind the open needs, held as one closure the way
            // the scan and the counter are: the store and the listing it already has,
            // so it neither lists the prefix again nor has to be told what it is
            // reading.
            let (store, cloud_opts, runtime) = (store.clone(), cloud_opts.clone(), runtime.clone());
            let (files, full) = (files.clone(), full.to_string());
            // The same meter the open is writing into, not a new one: this pass reads
            // the dataset's footers over again — including the two ends the open
            // already read, since the whole dataset is built from one set of them, and
            // a sample of them past `MAX_FOOTER_READS` — and what the footers cost is
            // both passes added up, re-reads and all. It is held rather than handed
            // in because it belongs to this dataset: the next open builds its own state
            // and its own meter, and this closure goes with the state it was built for.
            let meter = report.meter.clone();
            // This is the pass that reads a large dataset's footers, so this is where a
            // large dataset gets remembered. The open above it has read two and has
            // nothing worth keeping; leaving the saving there meant the cache only ever
            // held datasets small enough to open in one wave — the ones that cost least
            // to read in the first place.
            let remembered = report.remembered.clone();
            let fingerprint = fingerprint.clone();
            state.set_footers_pending(Arc::new(move |progress: &Arc<_>| {
                let read = crate::schema_union::footers_to_read(files.len());
                let footers = Self::cloud_footers(
                    store.clone(),
                    files.clone(),
                    read.clone(),
                    &runtime,
                    progress.clone(),
                    meter.clone(),
                )?;
                Self::remember_dataset_shape(
                    remembered.as_ref(),
                    &full,
                    &fingerprint,
                    &read,
                    &files,
                    &footers,
                );
                let whole = Self::cloud_dataset_from_footers(
                    &full,
                    &root,
                    &files,
                    &read,
                    &footers,
                    &cloud_opts,
                )?;
                // The same exclusion the open makes: a footer that would not read on
                // this pass either is a file Polars cannot read, and scanning it takes
                // the prefix down. This pass can find one the open could not — it only
                // read two footers — so the exclusion belongs on both sides.
                let readable =
                    crate::schema_union::readable_paths(&whole.urls, &whole.dataset.unreadable)
                        .into_owned();
                let lf = (whole.scan)(&readable, &[]).ok()?;
                // Over the same files, so the count it answers with fits the list the
                // dataset is about to hold.
                let counted: Vec<cloud_hive::DatasetFile> = files
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| whole.dataset.unreadable.binary_search(index).is_err())
                    .map(|(_, file)| file.clone())
                    .collect();
                if counted.len() != readable.len() {
                    return None;
                }
                let count: crate::widgets::datatable::FileCounter = {
                    let (runtime, counted, store) =
                        (runtime.clone(), Arc::new(counted), store.clone());
                    // As above: counting re-reads every footer, and those reads count.
                    let meter = meter.clone();
                    Arc::new(move || {
                        let (store, counted, meter) =
                            (store.clone(), counted.clone(), meter.clone());
                        wait_on_runtime(&runtime, async move {
                            cloud_hive::row_groups_of_files(&store, &counted, &meter).await
                        })
                        .ok_or_else(|| "cancelled".to_string())?
                        .map_err(|e| e.to_string())
                    })
                };
                Some(crate::widgets::datatable::FootersFound {
                    // What the listing passed over travels with the pass, or the note
                    // about it is on screen from the open and gone the moment the
                    // columns join — which on a prefix of more than a wave of files is
                    // every prefix there is.
                    dataset: whole.dataset.with_skipped(skipped),
                    lf,
                    file_rows: whole.file_rows,
                    // Every file listed: the dataset's per-file findings index this.
                    files: whole.urls,
                    row_groups: whole.row_groups,
                    remote: Some(crate::widgets::datatable::RemoteRead {
                        urls: readable,
                        scan: whole.scan,
                        count,
                    }),
                })
            }));
        }
        Some(state)
    }

    /// The footers at `read`, fetched on the runtime. `None` if the open was abandoned.
    ///
    /// Everything is cloned into the future rather than borrowed: it outlives this
    /// frame, and the counter is shared with whoever is rendering anyway.
    #[cfg(feature = "cloud")]
    fn cloud_footers(
        store: Arc<dyn object_store::ObjectStore>,
        files: Arc<Vec<cloud_hive::DatasetFile>>,
        read: Vec<usize>,
        runtime: &tokio::runtime::Handle,
        progress: Arc<crate::schema_union::FooterProgress>,
        meter: Arc<crate::measurements::Meter>,
    ) -> Option<Vec<Option<cloud_hive::FileFooter>>> {
        wait_on_runtime(runtime, async move {
            cloud_hive::footers_of_files_reporting(&store, &files, &read, &progress, &meter).await
        })
    }

    /// What a set of a cloud dataset's footers says, and the scan that reads it.
    ///
    /// Shared by the open, which has read the two ends, and the pass behind it, which
    /// has read them all: the two differ only in how much they know, and a dataset
    /// built from a sample already says so — it forgoes numbering its rows and scopes
    /// its notes to the footers it saw.
    #[cfg(feature = "cloud")]
    /// Keep what this pass learned, if it learned the whole of it.
    ///
    /// Two conditions, and both matter.
    ///
    /// Every footer must have been read. A staged open has read two of them and a
    /// sampled one a spread, and either kept as though it were the whole dataset would
    /// hand the next open a smaller dataset than it asked for, with nothing to say that
    /// is what happened.
    ///
    /// Every footer must have *parsed*. A footer read can fail because the file is
    /// corrupt, and it can fail because the store throttled the request or a token
    /// expired — and nothing here can tell those apart. Remembering the failure turns a
    /// moment's trouble into a file that is missing from the dataset on every open from
    /// now until something else in the prefix changes, which is not a trade a cache is
    /// allowed to make. Read them again next time; the one that was really corrupt
    /// costs a read and says the same thing.
    fn remember_dataset_shape(
        cache: Option<&crate::cache::CacheManager>,
        full: &str,
        fingerprint: &str,
        read: &[usize],
        files: &[cloud_hive::DatasetFile],
        footers: &[Option<cloud_hive::FileFooter>],
    ) {
        let Some(cache) = cache else {
            return;
        };
        // The dataset index too, which is what the home screen reads. A dataset opened
        // straight from a bucket used to be recorded here, by the URL it was opened as,
        // and nowhere else — so its recent row showed no shape, no size and no label,
        // and looked broken beside the local rows. Written before the shape, because a
        // sampled read still says what the columns are, and the shape below wants
        // every footer. A sampled read does not replace a whole one, though: the shape
        // cache is the smaller of the two and forgets a dataset long before the index
        // does, and a reopen that finds its shape gone reads a sample first.
        if let Some((path, facts)) = Self::facts_from_cloud_footers(full, files, read, footers) {
            let existing = cache.load_dataset_facts();
            if Self::facts_worth_recording(existing.get(&path), &facts) {
                cache.record_dataset_facts(&[(path, facts)]);
            }
        }
        if read.len() != files.len() || !footers.iter().all(Option::is_some) {
            return;
        }
        let (cached, schemas) = cloud_hive::footers_to_cache(footers);
        cache.save_dataset_shape(
            full,
            crate::cache::DatasetShape {
                fingerprint: fingerprint.to_string(),
                files: cached,
                schemas,
                taken_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or_default(),
            },
        );
    }

    /// What the home screen can say about a cloud dataset from the footers an open
    /// read: its columns, its rows when every footer was read, its kind, and what it
    /// holds. Keyed by the URL as opened, which is what the recents store holds.
    ///
    /// A remote row has no fingerprint to check, so `mtime` is the newest object's
    /// stamp and `size` the total, for the record's own sake.
    #[cfg(feature = "cloud")]
    fn facts_from_cloud_footers(
        full: &str,
        files: &[cloud_hive::DatasetFile],
        read: &[usize],
        footers: &[Option<cloud_hive::FileFooter>],
    ) -> Option<(PathBuf, crate::cache::DatasetFacts)> {
        let (dataset, partition_columns) =
            cloud_hive::dataset_schema_from_footers(files, read, footers).ok()?;
        let columns: Vec<String> = dataset
            .schema
            .iter_names()
            .map(|name| name.to_string())
            .collect();
        let every_footer = read.len() == files.len() && footers.iter().all(Option::is_some);
        let rows = every_footer.then(|| {
            footers
                .iter()
                .flatten()
                .map(|f| f.row_group_rows.iter().sum::<usize>())
                .sum()
        });
        // A directory of files, unless the listing came back with the one object the URL
        // names — which is what `--hive` on a single object gets. The trailing slash is
        // not asked about: this route is entered for `--hive s3://bucket/sales` too.
        let directory = !(files.len() == 1 && full.trim_end_matches('/').ends_with(&files[0].key));
        let kind = if !directory {
            discover::EntryKind::File
        } else if !partition_columns.is_empty() {
            discover::EntryKind::Hive
        } else {
            discover::EntryKind::MultiFile
        };
        let holds = if directory {
            discover::Holds {
                formats: vec![("parquet".to_string(), files.len())],
                ..Default::default()
            }
        } else {
            Default::default()
        };
        Some((
            PathBuf::from(full),
            crate::cache::DatasetFacts {
                mtime: files.iter().map(|f| f.stamp).max().unwrap_or_default(),
                size: files.iter().map(|f| f.size).sum(),
                rows,
                cols: Some(columns.len()),
                cols_sampled: !every_footer,
                columns,
                kind: Some(kind),
                classified_by: discover::CLASSIFIER_VERSION,
                cost: Default::default(),
                holds,
            },
        ))
    }

    /// Whether a record learned from a cloud open should replace what the index has:
    /// anything replaces nothing, a whole read replaces anything, and a sampled read
    /// replaces only another sample.
    #[cfg(feature = "cloud")]
    fn facts_worth_recording(
        existing: Option<&crate::cache::DatasetFacts>,
        new: &crate::cache::DatasetFacts,
    ) -> bool {
        match existing {
            None => true,
            Some(_) if new.rows.is_some() => true,
            Some(old) => old.rows.is_none(),
        }
    }

    /// What the home screen can say about one object opened from a bucket: its rows
    /// and columns from the footer the open read, under the URL it resolved to. The
    /// object's size is not known here — the footer is read from the tail — so the
    /// record carries none, and the row shows none. Its `mtime` is the time of the
    /// open: a remote record is never fingerprinted by it, and the index evicts its
    /// oldest `mtime` first, so a zero would make these the first to go.
    #[cfg(feature = "cloud")]
    fn record_cloud_object_facts(
        cache: Option<&crate::cache::CacheManager>,
        full: &str,
        footer: &cloud_hive::ParquetFooter,
    ) {
        let Some(cache) = cache else {
            return;
        };
        let columns: Vec<String> = footer
            .schema
            .iter_names()
            .map(|name| name.to_string())
            .collect();
        cache.record_dataset_facts(&[(
            PathBuf::from(full),
            crate::cache::DatasetFacts {
                mtime: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or_default(),
                size: 0,
                rows: Some(footer.row_group_rows.iter().sum()),
                cols: Some(columns.len()),
                cols_sampled: false,
                columns,
                kind: Some(discover::EntryKind::File),
                classified_by: discover::CLASSIFIER_VERSION,
                cost: discover::Cost {
                    row_groups: Some(footer.row_group_rows.len()),
                    ..Default::default()
                },
                holds: Default::default(),
            },
        )]);
    }

    #[cfg(feature = "cloud")]
    fn cloud_dataset_from_footers(
        full: &str,
        // The literal part of `full`, which for a glob is everything before its star.
        // The layout and column-range notes work by taking each file's path relative to
        // the dataset's root, so a root with a star in it is a prefix of nothing and
        // every note goes quietly empty.
        root: &str,
        files: &[cloud_hive::DatasetFile],
        read: &[usize],
        footers: &[Option<cloud_hive::FileFooter>],
        cloud_opts: &CloudOptions,
    ) -> Option<CloudDataset> {
        let (dataset, partition_columns) =
            cloud_hive::dataset_schema_from_footers(files, read, footers).ok()?;
        let urls: Vec<String> = files
            .iter()
            .filter_map(|f| cloud_hive::url_of_key(full, &f.key))
            .collect();
        if urls.is_empty() || urls.len() != files.len() {
            return None;
        }
        // A file that stores a column in a type the dataset's column cannot hold is not
        // read for it; its rows are null there rather than failing the scan, and carry
        // their file's drift group so the null can be told from a real one.
        let file_rows: Vec<usize> = if read.len() == files.len() {
            footers
                .iter()
                .map(|f| f.as_ref().map(|f| f.row_group_rows.iter().sum()))
                .collect::<Option<Vec<_>>>()
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        let row_groups: Vec<Vec<usize>> =
            if read.len() == files.len() && footers.iter().all(Option::is_some) {
                footers
                    .iter()
                    .flatten()
                    .map(|f| f.row_group_rows.clone())
                    .collect()
            } else {
                Vec::new()
            };
        let drift = crate::schema_union::ScanDrift::new(&urls, &dataset, &file_rows);
        let schema = dataset.schema.clone();
        let scan: crate::widgets::datatable::FileScan = {
            let (schema, partition_columns, drift, cloud_opts) = (
                schema.clone(),
                partition_columns.clone(),
                drift.map(Arc::new),
                cloud_opts.clone(),
            );
            Arc::new(
                move |urls: &[String], as_text: &[polars::prelude::PlSmallStr]| {
                    let drifts = drift.is_some();
                    cloud_hive::lenient_scan(
                        urls,
                        schema.clone(),
                        Some(cloud_opts.clone()),
                        drift.as_deref(),
                        as_text,
                    )
                    .map(|lf| {
                        Self::hoist_partition_columns(lf, &schema, &partition_columns, drifts)
                    })
                },
            )
        };
        Some(CloudDataset {
            dataset: dataset.with_partition_layouts(root, &urls),
            file_rows,
            urls,
            row_groups,
            scan,
            partition_columns,
        })
    }

    /// General schema route: ask the frame itself. Slow for a wide hive dataset, which
    /// is the reason this whole phase belongs on a background thread.
    fn schema_state_from_full_scan(
        mut lf: LazyFrame,
        path: Option<&Path>,
        options: &OpenOptions,
    ) -> Result<DataTableState> {
        let schema = lf
            .collect_schema()
            .map_err(color_eyre::eyre::Report::from)?;
        let partition_columns = match path.filter(|p| {
            options.hive && (p.is_dir() || p.as_os_str().to_string_lossy().contains('*'))
        }) {
            Some(p) => DataTableState::discover_hive_partition_columns(p)
                .into_iter()
                .filter(|c| schema.contains(c.as_str()))
                .collect::<Vec<_>>(),
            None => Vec::new(),
        };
        let lf = Self::hoist_partition_columns(lf, &schema, &partition_columns, false);
        let part_cols = (!partition_columns.is_empty()).then_some(partition_columns);
        DataTableState::from_schema_and_lazyframe(schema, lf, options, part_cols)
    }

    /// Build the table state for a loaded frame, by the cheapest route that applies.
    ///
    /// Returns the state and a label naming the route it came from, for the debug
    /// overlay. Takes its config by value so all of it can run off the UI thread.
    fn build_schema_state(
        lf: LazyFrame,
        path: Option<&Path>,
        options: &OpenOptions,
        cloud: &crate::config::CloudConfig,
        runtime: &tokio::runtime::Handle,
        report: &crate::measurements::OpenReport,
    ) -> Result<(DataTableState, String)> {
        let (mut state, label, meter) =
            Self::schema_state_by_route(lf, path, options, cloud, runtime, report)?;
        // The dataset leaves with the meter of the route that actually built it, so it
        // is installed with the dataset and nothing else can reach it. An open that
        // fails never gets here, which is what keeps the dataset still on screen
        // showing its own figures.
        state.set_measurements(meter);
        // What the open did, as against what it found. The one place both are known:
        // the scan has reported what it passed over, the caller has said whether this
        // is a lake table's plain files, and the state that will carry the notes is in
        // hand. See `DataTableState::open_notes` for why they are not the other notes.
        // A delimited file read with a header whose names are all numbers: its first
        // row of data, most likely, which `H` reads as data instead.
        let names_look_like_data = options.format.and_then(FileFormat::separator).is_some()
            && options.has_header != Some(false)
            && !crate::schema_union::names_are_names(
                &state
                    .schema
                    .iter_names()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>(),
            );
        state.set_open_notes(crate::notes::from_the_open(
            &options.left_out,
            options.read_as_plain_files_of,
            options.files_disagree,
            names_look_like_data,
        ));
        // And the half of it that cannot be missed: the row count on screen is a true
        // count of the files and a wrong one of the table.
        state.set_not_the_table(options.read_as_plain_files_of);
        // The display path of a downloaded object is its URL too; only a scan that
        // really reads the object store in place buffers like one.
        if path.is_some_and(source::scans_in_place) {
            state.set_remote_source();
        }
        Ok((state, label))
    }

    /// Scan a prefix in an object store with the reader its format calls for.
    ///
    /// Every cloud path went to `scan_parquet` whatever was under it, so a prefix of
    /// CSV came back "Could not read from S3. Check credentials and URL" — a false
    /// statement about the user's login, made about a directory datui could see the
    /// contents of. Polars' other scans take the same `CloudOptions` and do their own
    /// listing; nothing was passing them.
    ///
    /// Parquet keeps its own branch at each call site: it is the only one with hive
    /// partitioning, which is a Parquet-only capability in this reader, and it is the
    /// path every cloud dataset took before this existed.
    ///
    /// `None` when the format is not one of these, which sends the caller back to the
    /// Parquet scan it always made.
    #[cfg(feature = "cloud")]
    fn scan_cloud_prefix(
        url: &str,
        cloud_opts: CloudOptions,
        format: FileFormat,
        glob: bool,
        options: &OpenOptions,
    ) -> Option<Result<LazyFrame>> {
        use polars::prelude::{LazyCsvReader, LazyFileListReader};
        // A plain prefix is narrowed to the keys with an extension. A console's folder
        // marker comes back from the listing as `data` for `data/`, which Polars reads
        // as a file of a different kind from the rest and refuses the whole prefix.
        let pl_path = if url.ends_with('/') && !url.contains('*') {
            PlRefPath::new(format!("{url}**/*.*").as_str())
        } else {
            PlRefPath::new(url)
        };
        let named = |e: polars::error::PolarsError| {
            color_eyre::eyre::eyre!("Could not read {} as {}: {e}", url, format.name())
        };
        let lf = match format {
            FileFormat::Csv => {
                // The flags the user gave mean what they mean for a local file.
                let reader = || {
                    LazyCsvReader::new(pl_path.clone())
                        .with_cloud_options(Some(cloud_opts.clone()))
                        .with_glob(glob)
                };
                let nv = match DataTableState::build_null_values_with(options, || {
                    DataTableState::csv_schema_for_null_values(reader(), options)
                }) {
                    Ok(nv) => nv,
                    Err(e) => return Some(Err(e)),
                };
                // No `--parse-strings` here: its sample would be a second read of
                // the bucket. Nor Polars' `try_parse_dates`, which fails the whole
                // read on a value it cannot parse, even one like those it inferred
                // the type from. Timestamps stay text.
                DataTableState::configure_csv_reader(reader(), options, nv.as_ref())
                    .finish()
                    .map_err(named)
                    .and_then(|lf| {
                        DataTableState::apply_skip_tail_rows_csv(lf, options).map_err(|e| {
                            e.wrap_err(format!("Could not read {} as {}", url, format.name()))
                        })
                    })
            }
            FileFormat::Jsonl => polars::prelude::LazyJsonLineReader::new(pl_path)
                .with_cloud_options(Some(cloud_opts))
                .finish()
                .map_err(named),
            // Parquet has its own branch, and the rest have no multi-file cloud reader
            // in Polars — an ORC or Avro prefix is still a file at a time.
            _ => return None,
        };
        Some(lf)
    }

    /// The plain URL and Polars options for one object-store path, through the source
    /// it names or belongs to (`cloud_sources::resolve`).
    #[cfg(feature = "cloud")]
    fn resolve_cloud_url(
        path: &Path,
        cloud: &crate::config::CloudConfig,
    ) -> Result<(String, CloudOptions)> {
        let text = path.to_string_lossy();
        let resolved = crate::cloud_sources::resolve_for_open(&text, cloud)
            .map_err(|e| color_eyre::eyre::eyre!(e))?;
        let options = match resolved.kind {
            crate::cloud_browse::ProviderKind::S3 => Self::build_s3_cloud_options(&resolved.s3),
            crate::cloud_browse::ProviderKind::Gcs
                if resolved.signing == crate::cloud_sources::Signing::Unsigned =>
            {
                CloudOptions::default()
                    .with_gcp([(polars::io::cloud::GoogleConfigKey::SkipSignature, "true")])
            }
            crate::cloud_browse::ProviderKind::Gcs => match &resolved.gcloud {
                // The token comes from `gcloud` whenever Polars asks, so a long scan
                // outlives the one fetched here.
                Some((configuration, _)) => CloudOptions::default()
                    .with_credential_provider(Some(crate::gcloud::polars_provider(configuration))),
                None => match &resolved.google_credentials {
                    Some(file) => CloudOptions::default().with_gcp([(
                        polars::io::cloud::GoogleConfigKey::ApplicationCredentials,
                        file.to_string_lossy().into_owned(),
                    )]),
                    None => CloudOptions::default(),
                },
            },
            crate::cloud_browse::ProviderKind::Azure => {
                let (account, _, _) = source::azure_parts(&resolved.url)
                    .ok_or_else(|| color_eyre::eyre::eyre!("not an Azure URL"))?;
                CloudOptions::default()
                    .with_azure(crate::azure::polars_options(&account, &resolved.azure))
            }
        };
        Ok((resolved.url, options))
    }

    /// The URL, Polars options and store for one object-store path. `cloud` is the
    /// effective config the `App` keeps (see `OpenOptions::effective_cloud`).
    #[cfg(feature = "cloud")]
    fn cloud_store_for(
        path: &Path,
        cloud: &crate::config::CloudConfig,
        runtime: &tokio::runtime::Handle,
    ) -> Result<(String, CloudOptions, Arc<dyn object_store::ObjectStore>)> {
        let (full, cloud_opts) = Self::resolve_cloud_url(path, cloud)?;
        let store = Self::polars_object_store(&full, &cloud_opts, runtime)?;
        Ok((full, cloud_opts, store))
    }

    /// One Parquet object read in place: schema and row count from its footer, in one
    /// tail read through one store.
    ///
    /// Asking the frame for its schema fetched the footer through Polars, and Polars
    /// answers `len()` on a cloud scan by reading the first row group rather than the
    /// footer, so the background count that followed an open downloaded row group 0 a
    /// second time, alongside the buffer that was showing it. The footer has both
    /// answers for one 256 KiB range request; the schema is handed to the scan so
    /// Polars does not fetch it again, and the count is known before the first frame.
    /// A failure is returned, not swallowed: the caller falls back to asking the frame
    /// and puts the reason in the debug label.
    #[cfg(feature = "cloud")]
    fn schema_state_from_cloud_object(
        path: &Path,
        options: &OpenOptions,
        cloud: &crate::config::CloudConfig,
        runtime: &tokio::runtime::Handle,
        report: &crate::measurements::OpenReport,
    ) -> Result<DataTableState> {
        let (full, cloud_opts, store) = Self::cloud_store_for(path, cloud, runtime)?;
        let (_bucket, key) = Self::cloud_bucket_and_key(&full)?;
        if key.is_empty() {
            return Err(color_eyre::eyre::eyre!("a bucket, not an object"));
        }
        let meter = report.meter.clone();
        let footer = wait_on_runtime(runtime, async move {
            cloud_hive::footer_of_cloud_parquet(store, &key, &meter).await
        })
        .ok_or_else(|| color_eyre::eyre::eyre!("cancelled"))??;
        let args = ScanArgsParquet {
            schema: Some(footer.schema.clone()),
            cloud_options: Some(cloud_opts),
            hive_options: polars::io::HiveOptions::default(),
            glob: false,
            ..Default::default()
        };
        let lf = LazyFrame::scan_parquet(PlRefPath::new(full.as_str()), args)?;
        let mut state =
            DataTableState::from_schema_and_lazyframe(footer.schema.clone(), lf, options, None)?;
        state.set_row_groups(&footer.row_group_rows);
        // The commonest cloud open, and the one the dataset index never heard about:
        // the prefix route records what it read, and this one read a footer too.
        Self::record_cloud_object_facts(report.remembered.as_ref(), &full, &footer);
        state.set_column_widths(footer.column_bytes_per_row);
        Ok(state)
    }

    /// The schema routes, cheapest first: one local footer, one cloud footer (a hive
    /// prefix or a single object), then asking the frame.
    fn schema_state_by_route(
        lf: LazyFrame,
        path: Option<&Path>,
        options: &OpenOptions,
        cloud: &crate::config::CloudConfig,
        runtime: &tokio::runtime::Handle,
        report: &crate::measurements::OpenReport,
    ) -> Result<(DataTableState, String, Arc<crate::measurements::Meter>)> {
        #[cfg(not(feature = "cloud"))]
        let _ = (cloud, runtime);

        // A meter per attempt, and the winner's is the open's. The routes are tried in
        // order and the earlier ones measure before they discover they cannot finish —
        // the local hive route times its walk and its footer pass, then bails five
        // different ways. Sharing one meter would leave those figures on a dataset some
        // later route built, which is a row saying no files on a dataset that has them.
        //
        // Two guards, and the test holds them together rather than either alone: the
        // attempts take separate meters, and both full-scan arms hand back an empty
        // one. A directory whose only Parquet is a writer's own bookkeeping — a
        // `_delta_log` checkpoint — reaches the screen through the second of those, so
        // `test_a_route_that_gave_up_leaves_no_figures_on_the_dataset_that_opened`
        // fails when both are reverted and passes when either still stands. The
        // per-attempt meter alone is defensive: the route guards are mutually
        // exclusive enough that nothing reaches a later route through the first.
        let attempt = |report: &crate::measurements::OpenReport| crate::measurements::OpenReport {
            progress: report.progress.clone(),
            meter: Arc::new(crate::measurements::Meter::default()),
            remembered: report.remembered.clone(),
        };

        let local = attempt(report);
        if let Some(state) =
            Self::schema_state_from_local_hive(path, options, &local.progress, &local.meter)
        {
            return Ok((state, "one-file (local)".to_string(), local.meter));
        }
        #[cfg(feature = "cloud")]
        let cloud_hive_attempt = attempt(report);
        #[cfg(feature = "cloud")]
        if let Some(state) =
            Self::schema_state_from_cloud_hive(path, options, cloud, runtime, &cloud_hive_attempt)
        {
            return Ok((
                state,
                "one-file (cloud)".to_string(),
                cloud_hive_attempt.meter,
            ));
        }
        #[cfg(feature = "cloud")]
        if let Some(p) = path.filter(|p| {
            source::scans_in_place(p)
                && !options.hive
                && !source::is_prefix_or_glob(&p.to_string_lossy())
        }) {
            let object = attempt(report);
            match Self::schema_state_from_cloud_object(p, options, cloud, runtime, &object) {
                Ok(state) => return Ok((state, "footer (cloud)".to_string(), object.meter)),
                // Visible in the debug overlay, because the fallback costs a row group
                // for the count and that should not pass for the intended path.
                Err(e) => {
                    // A fresh meter, not the failed footer read's: the full scan
                    // measures nothing, and showing the attempt that did not work
                    // would describe a route the dataset did not come by.
                    return Self::schema_state_from_full_scan(lf, path, options).map(|state| {
                        (
                            state,
                            format!("full scan (cloud footer: {e})"),
                            Arc::new(crate::measurements::Meter::default()),
                        )
                    });
                }
            }
        }
        Self::schema_state_from_full_scan(lf, path, options).map(|state| {
            (
                state,
                "full scan".to_string(),
                Arc::new(crate::measurements::Meter::default()),
            )
        })
    }

    /// Build the LazyFrame for `paths`.
    ///
    /// Takes the cloud config by reference rather than reading `self`, so the same
    /// code can run on a background thread — scanning is where the wall-clock time
    /// goes for CSV (schema inference) and for hive directories with many files.
    /// Whether the files about to be read as one table do not all carry the same
    /// columns, for the note that says so.
    ///
    /// Only for the formats with no footer. A Parquet dataset's footers are read anyway
    /// and produce the exact version of this — which columns, in how many files, and
    /// where — so a second, vaguer note above those would be noise.
    ///
    /// A spread of the files rather than all of them, the same three
    /// [`crate::schema_union::sample_files`] reads for the label, and for the same
    /// reason: this runs on the way into a read the user is waiting for.
    fn files_disagree(
        files: &[PathBuf],
        options: &OpenOptions,
        found: FileFormat,
    ) -> crate::schema_union::Disagreement {
        // The format the read will use, not the one the names suggested: an explicit
        // `--format` outranks both, and judging a directory with a reader the open will
        // not use is a note about a read that never happened.
        let format = options.format.unwrap_or(found);
        if format == FileFormat::Parquet {
            return Default::default();
        }
        // Null values are the one setting the sample cannot mirror: `--null-value`
        // takes `COL=VAL` forms the reader resolves against the file it is opening, and
        // a sample that guessed would report a widening the table never did. They are
        // unset unless the user names them, so this stands down where it must and runs
        // everywhere else.
        if options.null_values.is_some() {
            return Default::default();
        }
        crate::schema_union::sample_files(files, format, &Self::read_as(options)).disagreement()
    }

    /// The reader settings a sample has to copy to describe what the open will do.
    ///
    /// Taken from the options the open is actually being made with, not guessed at and
    /// then bailed out of: `from_args_and_config` fills in `infer_schema_length` and
    /// `parse_strings` on every run with no flags at all, so a predicate over "did the
    /// user set anything" is true every time. That shipped once, and the notes about
    /// how a directory had been stacked never appeared outside the tests.
    fn read_as(options: &OpenOptions) -> crate::schema_union::ReadAs {
        crate::schema_union::ReadAs {
            delimiter: options.delimiter,
            has_header: options.has_header,
            skip_rows: options.skip_rows,
            skip_lines: options.skip_lines,
            infer_schema_length: options.infer_schema_length,
            ignore_errors: options.ignore_errors,
            try_parse_dates: options.csv_try_parse_dates(),
        }
    }

    /// `found` is what the read has to say about itself, for the caller to put in the
    /// dataset's notes: which data files it passed over, and whether the files it did
    /// read carry the same columns. Written here rather than worked out by the caller
    /// because this is the pass that decides, and a second opinion formed from a second
    /// directory read is a second answer waiting to disagree.
    fn build_lazyframe_from_paths_with(
        cloud: &crate::config::CloudConfig,
        paths: &[PathBuf],
        options: &OpenOptions,
        report: &mut ReadReport,
    ) -> Result<LazyFrame> {
        let path = &paths[0];
        match source::input_source(path) {
            source::InputSource::Http(_url) => {
                #[cfg(feature = "http")]
                {
                    return Err(color_eyre::eyre::eyre!(
                        "HTTP/HTTPS load is handled in the event loop; this path should not be reached."
                    ));
                }
                #[cfg(not(feature = "http"))]
                {
                    return Err(color_eyre::eyre::eyre!(
                        "HTTP/HTTPS URLs are not supported in this build. Rebuild with default features."
                    ));
                }
            }
            source::InputSource::S3(url) => {
                #[cfg(feature = "cloud")]
                {
                    let (full, cloud_opts) =
                        Self::resolve_cloud_url(Path::new(&format!("s3://{url}")), cloud)?;
                    let is_glob = source::is_prefix_or_glob(&full);
                    // The reader the prefix's own format calls for, when the listing
                    // said what that is. Only Parquet falls through to the scan below.
                    if let Some(format) = options.format.filter(|f| *f != FileFormat::Parquet)
                        && let Some(lf) = Self::scan_cloud_prefix(
                            &full,
                            cloud_opts.clone(),
                            format,
                            is_glob,
                            options,
                        )
                    {
                        return lf;
                    }
                    let pl_path = PlRefPath::new(full.as_str());
                    let hive_options = if is_glob {
                        polars::io::HiveOptions::new_enabled()
                    } else {
                        polars::io::HiveOptions::default()
                    };
                    let args = ScanArgsParquet {
                        cloud_options: Some(cloud_opts),
                        hive_options,
                        glob: is_glob,
                        ..Default::default()
                    };
                    let lf = LazyFrame::scan_parquet(pl_path, args).map_err(|e| {
                        color_eyre::eyre::eyre!(
                            "Could not read from S3. Check credentials and URL: {}",
                            e
                        )
                    })?;
                    // The frame alone. Building a state here would ask Polars for the
                    // schema, which lists every file under a prefix, and the schema
                    // phase that follows lists them once more for itself.
                    return Ok(lf);
                }
                #[cfg(not(feature = "cloud"))]
                {
                    let _ = url;
                    return Err(color_eyre::eyre::eyre!(
                        "S3 is not supported in this build. Rebuild with default features and set AWS credentials (e.g. AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)."
                    ));
                }
            }
            source::InputSource::Gcs(url) => {
                #[cfg(feature = "cloud")]
                {
                    let (full, cloud_opts) =
                        Self::resolve_cloud_url(Path::new(&format!("gs://{url}")), cloud)?;
                    let is_glob = source::is_prefix_or_glob(&full);
                    // The reader the prefix's own format calls for, when the listing
                    // said what that is. Only Parquet falls through to the scan below.
                    if let Some(format) = options.format.filter(|f| *f != FileFormat::Parquet)
                        && let Some(lf) = Self::scan_cloud_prefix(
                            &full,
                            cloud_opts.clone(),
                            format,
                            is_glob,
                            options,
                        )
                    {
                        return lf;
                    }
                    let pl_path = PlRefPath::new(full.as_str());
                    let hive_options = if is_glob {
                        polars::io::HiveOptions::new_enabled()
                    } else {
                        polars::io::HiveOptions::default()
                    };
                    let args = ScanArgsParquet {
                        cloud_options: Some(cloud_opts),
                        hive_options,
                        glob: is_glob,
                        ..Default::default()
                    };
                    let lf = LazyFrame::scan_parquet(pl_path, args).map_err(|e| {
                        color_eyre::eyre::eyre!(
                            "Could not read from GCS. Check credentials and URL: {}",
                            e
                        )
                    })?;
                    return Ok(lf);
                }
                #[cfg(not(feature = "cloud"))]
                {
                    let _ = url;
                    return Err(color_eyre::eyre::eyre!(
                        "GCS (gs://) is not supported in this build. Rebuild with default features."
                    ));
                }
            }
            source::InputSource::Azure(url) => {
                #[cfg(feature = "cloud")]
                {
                    let (full, cloud_opts) = Self::resolve_cloud_url(Path::new(&url), cloud)?;
                    let is_glob = source::is_prefix_or_glob(&full);
                    // The reader the prefix's own format calls for, when the listing
                    // said what that is. Only Parquet falls through to the scan below.
                    if let Some(format) = options.format.filter(|f| *f != FileFormat::Parquet)
                        && let Some(lf) = Self::scan_cloud_prefix(
                            &full,
                            cloud_opts.clone(),
                            format,
                            is_glob,
                            options,
                        )
                    {
                        return lf;
                    }
                    let args = ScanArgsParquet {
                        cloud_options: Some(cloud_opts),
                        hive_options: if is_glob {
                            polars::io::HiveOptions::new_enabled()
                        } else {
                            polars::io::HiveOptions::default()
                        },
                        glob: is_glob,
                        ..Default::default()
                    };
                    let lf = LazyFrame::scan_parquet(PlRefPath::new(full.as_str()), args).map_err(
                        |e| {
                            color_eyre::eyre::eyre!(
                                "Could not read from Azure. Check credentials and URL: {}",
                                e
                            )
                        },
                    )?;
                    return Ok(lf);
                }
                #[cfg(not(feature = "cloud"))]
                {
                    let _ = url;
                    return Err(color_eyre::eyre::eyre!(
                        "Azure is not supported in this build. Rebuild with default features."
                    ));
                }
            }
            source::InputSource::Local(_) => {}
        }

        // One path that is a directory, whether or not `--hive` said so: naming a
        // directory is the request to read it, and the dispatch below is what picks the
        // reader for what it holds. Behind `options.hive` alone, every route that
        // reached here with a directory and without the flag fell through to the
        // Parquet scan and answered `Unsupported file type`.
        if paths.len() == 1 && (options.hive || path.is_dir()) {
            let path_str = path.as_os_str().to_string_lossy();
            let is_single_file = path.exists()
                && path.is_file()
                && !path_str.contains('*')
                && !path_str.contains("**");
            if !is_single_file {
                // What the directory holds picks the reader. A directory used to go
                // straight to the Parquet scan whatever was in it, so a directory of
                // `.json.gz` was opened by seeking each file's last four bytes for a
                // `PAR1` that was never going to be there — the files were fine, the
                // reader was never asked to be the right one.
                if path.is_dir() {
                    match crate::discover::directory_format(path) {
                        // Flat and Parquet: the scan below is already right for it.
                        crate::discover::DirectoryFormat::One(FileFormat::Parquet, _) => {}
                        // Partitions, or an empty directory. The files are a level down
                        // under `key=value` and only the hive scan walks a tree — but
                        // hive partitioning is a Parquet-only capability in the reader
                        // datui uses (`HiveOptions::new_disabled()` is hard-coded for
                        // CSV and NDJSON), so partitions of anything else cannot be
                        // read as one table here. Saying which files they are beats
                        // Parquet's complaint that they do not end with `PAR1`.
                        crate::discover::DirectoryFormat::Deeper => {
                            if let crate::discover::DirectoryFormat::One(found, files) =
                                crate::discover::hive_leaf_format(path)
                                && found != FileFormat::Parquet
                            {
                                // The extension rather than the format's own name: it
                                // is what is on the files the user can see.
                                let named = files
                                    .first()
                                    .and_then(|f| crate::discover::data_extension(f))
                                    .unwrap_or_else(|| format!("{found:?}").to_lowercase());
                                return Err(color_eyre::eyre::eyre!(
                                    "{} is partitioned into key=value directories of .{} \
                                     files. datui reads hive partitioning for Parquet \
                                     only — open one partition instead.",
                                    path.display(),
                                    named
                                ));
                            }
                        }
                        crate::discover::DirectoryFormat::One(found, files) => {
                            // Read as the files themselves, through the same readers a
                            // list of files typed on the command line goes through. An
                            // explicit `--format` is the user's own answer and outranks
                            // what the names say.
                            report.files_disagree = Self::files_disagree(&files, options, found);
                            let nested = OpenOptions {
                                hive: false,
                                format: Some(options.format.unwrap_or(found)),
                                ..options.clone()
                            };
                            return Self::build_lazyframe_from_paths_with(
                                cloud, &files, &nested, report,
                            );
                        }
                        crate::discover::DirectoryFormat::Mixed {
                            format: found,
                            files,
                            passed_over,
                        } => {
                            // The commonest format is the table. A directory of a
                            // thousand CSVs and one stray JSON is a directory of CSVs,
                            // and refusing the whole of it over the stray was datui
                            // deciding that a directory it could read was not worth
                            // reading.
                            report.files_disagree = Self::files_disagree(&files, options, found);
                            let nested = OpenOptions {
                                hive: false,
                                format: Some(options.format.unwrap_or(found)),
                                ..options.clone()
                            };
                            let lf = Self::build_lazyframe_from_paths_with(
                                cloud, &files, &nested, report,
                            )?;
                            // After the call, which reads a flat directory of one format
                            // and leaves nothing out of its own.
                            report.left_out = passed_over;
                            return Ok(lf);
                        }
                    }
                }
                let use_parquet_hive = path.is_dir()
                    || path_str.contains(".parquet")
                    || path_str.contains("*.parquet");
                if use_parquet_hive {
                    // Only build LazyFrame here; schema + partition discovery happen in DoLoadSchema ("Caching schema")
                    return DataTableState::scan_parquet_hive(path);
                }
                return Err(color_eyre::eyre::eyre!(
                    "With --hive use a directory or a glob pattern for Parquet (e.g. path/to/dir or path/**/*.parquet)"
                ));
            }
        }

        // A file with no extension may still be Parquet: a part file in a directory named
        // `.parquet`, or anything whose bytes say so. A regular file is only read when
        // nothing else settled it.
        let effective_format = options
            .format
            .or_else(|| FileFormat::from_path(path))
            .or_else(|| {
                (path.extension().is_none()
                    && (crate::discover::is_parquet_key(&path.to_string_lossy())
                        || (path.is_file() && crate::discover::has_parquet_magic(path))))
                .then_some(FileFormat::Parquet)
            });
        report.format = effective_format;

        let lf = if paths.len() > 1 {
            match effective_format {
                Some(FileFormat::Parquet) => DataTableState::from_parquet_paths(
                    paths,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Csv) => DataTableState::from_csv_paths(paths, options)?,
                Some(FileFormat::Json) => DataTableState::from_json_paths(
                    paths,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Jsonl) => DataTableState::from_json_lines_paths(
                    paths,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Arrow) => DataTableState::from_ipc_paths(
                    paths,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Avro) => DataTableState::from_avro_paths(
                    paths,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Orc) => DataTableState::from_orc_paths(
                    paths,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Tsv) | Some(FileFormat::Psv) | Some(FileFormat::Excel) | None => {
                    // The home screen asks `reads_many_files` before it offers a
                    // directory as one dataset, so a format that is refused here and
                    // offered there would be a promise nothing keeps. Asserted rather
                    // than restated: adding a format to this arm without the predicate
                    // fails every debug run. The other direction — dropping one from the
                    // predicate and not from here — this cannot see, and would hide a
                    // directory datui can read rather than promise one it cannot.
                    debug_assert!(
                        effective_format.is_none_or(|f| !f.reads_many_files()),
                        "this arm and FileFormat::reads_many_files must agree"
                    );
                    if !paths.is_empty() && !path.exists() {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("File not found: {}", path.display()),
                        )
                        .into());
                    }
                    return Err(color_eyre::eyre::eyre!(
                        "Unsupported file type for multiple files (parquet, csv, json, jsonl, ndjson, arrow/ipc/feather, avro, orc only)"
                    ));
                }
            }
        } else {
            match effective_format {
                Some(FileFormat::Parquet) => DataTableState::from_parquet(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Csv) => DataTableState::from_csv(path, options)?,
                Some(FileFormat::Tsv) => DataTableState::from_delimited(path, b'\t', options)?,
                Some(FileFormat::Psv) => DataTableState::from_delimited(path, b'|', options)?,
                Some(FileFormat::Json) => DataTableState::from_json(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Jsonl) => DataTableState::from_json_lines(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Arrow) => DataTableState::from_ipc(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Avro) => DataTableState::from_avro(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                Some(FileFormat::Excel) => DataTableState::from_excel(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                    options.excel_sheet.as_deref(),
                )?,
                Some(FileFormat::Orc) => DataTableState::from_orc(
                    path,
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    options.row_numbers,
                    options.row_start_index,
                )?,
                None => {
                    if paths.len() == 1 && !path.exists() {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("File not found: {}", path.display()),
                        )
                        .into());
                    }
                    return Err(color_eyre::eyre::eyre!("Unsupported file type"));
                }
            }
        };
        // JSON is read into memory whole, so its sample costs no read of the file.
        if matches!(
            effective_format,
            Some(FileFormat::Json) | Some(FileFormat::Jsonl)
        ) {
            return DataTableState::apply_parse_dates_to_json_lazyframe(lf.lf, options);
        }
        Ok(lf.lf)
    }

    /// Whether the plain help overlay is on screen.
    pub fn help_visible(&self) -> bool {
        self.show_help
    }

    /// Open the views list for the dataset on screen, scored against it.
    fn open_template_list(&mut self) {
        if self.data_table_state.is_none() || self.path.is_none() {
            return;
        }
        self.template_modal.table_state.select(Some(0));
        self.refresh_view_list();
        self.template_modal.active = true;
        self.template_modal.mode = TemplateModalMode::List;
    }

    /// Rebuild the list's rows from the store, scored and annotated against
    /// the open dataset; the selection stays near where it was.
    fn refresh_view_list(&mut self) {
        let (Some(state), Some(path)) = (&self.data_table_state, &self.path) else {
            return;
        };
        let rows: Vec<ViewRow> = self
            .template_manager
            .find_relevant_templates(path, state.source_schema())
            .into_iter()
            .map(|(template, score)| {
                let reason = template::match_reason(&template, path, state.source_schema());
                ViewRow {
                    template,
                    score,
                    reason,
                }
            })
            .collect();
        self.template_modal.broken_templates = self.template_manager.broken_templates.clone();
        let selected = self.template_modal.table_state.selected().unwrap_or(0);
        self.template_modal.table_state.select(if rows.is_empty() {
            None
        } else {
            Some(selected.min(rows.len() - 1))
        });
        self.template_modal.rows = rows;
    }

    /// Open the save-view form prefilled from the open dataset: a name the
    /// user will recognize, this file's paths and patterns as criteria, and
    /// schema match on — the criterion that carries the view to the next
    /// table shaped like this one.
    fn open_save_view_form(&mut self) {
        self.template_modal
            .enter_create_mode(self.history_limit, &self.theme);

        let query = self.data_table_state.as_ref().and_then(|state| {
            let (query, sql_query, fuzzy_query) = active_query_settings(
                state.get_active_query(),
                state.get_active_sql_query(),
                state.get_active_fuzzy_query(),
            );
            sql_query.or(fuzzy_query).or(query)
        });
        self.template_modal.name_input.set_value(
            self.template_manager
                .suggest_name(self.path.as_deref(), query.as_deref()),
        );

        if let Some(ref path) = self.path {
            // Pin this file: its absolute path or URL, its path relative to the
            // working directory when it is local and under it, and glob suggestions.
            let absolute_path = template::exact_location(path);
            self.template_modal
                .exact_path_input
                .set_value(absolute_path.to_string_lossy());
            if let Some(relative) = template::relative_location(path) {
                self.template_modal.relative_path_input.set_value(relative);
            }

            // Suggest a path pattern from the absolute path: the parent of a
            // bare relative name is "", and ""/*.parquet is a pattern that
            // matches every parquet file anywhere, forever. The separator is the
            // path's own, or a Windows path never fits its pattern.
            if let Some(parent) = absolute_path.parent()
                && let Some(parent_str) = parent.to_str()
                && !parent_str.is_empty()
                && let Some(ext) = absolute_path.extension()
            {
                let separator = if crate::source::is_remote_url(path) {
                    '/'
                } else {
                    std::path::MAIN_SEPARATOR
                };
                self.template_modal.path_pattern_input.set_value(format!(
                    "{}{separator}*.{}",
                    parent_str.trim_end_matches(separator),
                    ext.to_string_lossy()
                ));
            }

            // Suggest a filename pattern with digit runs wildcarded, so
            // sales_2024.csv offers itself to sales_2025.csv.
            if let Some(filename) = path.file_name()
                && let Some(filename_str) = filename.to_str()
            {
                use regex::Regex;
                let pattern = match Regex::new(r"\d+") {
                    Ok(re) => re.replace_all(filename_str, "*").to_string(),
                    Err(_) => filename_str.to_string(),
                };
                self.template_modal
                    .filename_pattern_input
                    .set_value(pattern);
            }
        }

        // Schema match starts on: "apply this to a similar table" is the
        // reason views exist, and the columns are the only criterion that
        // says similar.
        if let Some(ref state) = self.data_table_state
            && !state.source_schema().is_empty()
        {
            self.template_modal.schema_match_enabled = true;
        }
    }

    /// Validate and persist the form: a new view, or the edited one. The
    /// settings are rebuilt from the table's applied state either way. A
    /// failed save keeps the form open.
    fn save_view_form(&mut self) {
        self.template_modal.name_error = None;
        let name = self.template_modal.name_input.value().trim().to_string();
        if name.is_empty() {
            self.template_modal.name_error = Some("name is required".to_string());
            self.template_modal.form_focus = FormFocus::Name;
            return;
        }
        let renaming_to_taken = match &self.template_modal.editing_template_id {
            None => self.template_manager.template_exists(&name),
            Some(id) => self
                .template_manager
                .get_template_by_name(&name)
                .is_some_and(|other| other.id != *id),
        };
        if renaming_to_taken {
            self.template_modal.name_error = Some("name already exists".to_string());
            self.template_modal.form_focus = FormFocus::Name;
            return;
        }

        let non_empty = |input: &widgets::text_input::TextInput| {
            let value = input.value().trim();
            (!value.is_empty()).then(|| value.to_string())
        };
        let match_criteria = template::MatchCriteria {
            exact_path: non_empty(&self.template_modal.exact_path_input)
                .map(std::path::PathBuf::from),
            relative_path: non_empty(&self.template_modal.relative_path_input),
            path_pattern: non_empty(&self.template_modal.path_pattern_input),
            filename_pattern: non_empty(&self.template_modal.filename_pattern_input),
            // The columns the view's settings run on, not the query's output: the
            // next file is matched as loaded.
            schema_columns: if self.template_modal.schema_match_enabled {
                self.data_table_state.as_ref().map(|state| {
                    state
                        .source_schema()
                        .iter_names()
                        .map(|s| s.to_string())
                        .collect()
                })
            } else {
                None
            },
            schema_types: None,
        };
        let description = {
            let value = self.template_modal.description_input.value();
            (!value.is_empty()).then(|| value.to_string())
        };

        let saved = if let Some(editing_id) = self.template_modal.editing_template_id.clone() {
            let Some(mut template) = self
                .template_manager
                .get_template_by_id(&editing_id)
                .cloned()
            else {
                return;
            };
            template.name = name;
            template.description = description;
            let stored_schema = template.match_criteria.schema_columns.take();
            template.match_criteria = match_criteria;
            let editing_the_active_view =
                self.active_template_id.as_deref() == Some(editing_id.as_str());
            // The same principle as the settings below: editing an unapplied
            // view must not swap the columns it matches on for the columns of
            // whatever table happens to be open. The toggle still works — off
            // drops the criterion — and the active view follows its table.
            if !editing_the_active_view
                && self.template_modal.schema_match_enabled
                && stored_schema.is_some()
            {
                template.match_criteria.schema_columns = stored_schema;
            }
            // The settings follow the table only while this view is the one
            // dressing it. Editing an unapplied view changes its name,
            // description and matching alone — it must not overwrite what
            // the view carries with whatever the table happens to show.
            if editing_the_active_view && let Some(state) = &self.data_table_state {
                let (query, sql_query, fuzzy_query) = active_query_settings(
                    state.get_active_query(),
                    state.get_active_sql_query(),
                    state.get_active_fuzzy_query(),
                );
                template.settings = template::TemplateSettings {
                    query,
                    sql_query,
                    fuzzy_query,
                    filters: state.get_filters().to_vec(),
                    sort_columns: state.get_sort_columns().to_vec(),
                    sort_descending: state.get_sort_descending().to_vec(),
                    sort_ascending: state.get_sort_ascending(),
                    column_order: state.get_column_order().to_vec(),
                    locked_columns_count: state.locked_columns_count(),
                    pivot: state.last_pivot_spec().cloned(),
                    melt: state.last_melt_spec().cloned(),
                };
            }
            self.template_manager.update_template(&template).is_ok()
        } else {
            self.create_template_from_current_state(name, description, match_criteria)
                .is_ok()
        };
        if saved {
            self.refresh_view_list();
            self.template_modal.exit_form();
        }
    }

    /// The selected view's score breakdown, for the list's `i` popup.
    fn view_score_details(&self) -> Option<(String, String)> {
        let state = self.data_table_state.as_ref()?;
        let path = self.path.as_ref()?;
        let idx = self.template_modal.table_state.selected()?;
        let row = self.template_modal.rows.get(idx)?;
        let template = &row.template;

        let exact_path_match = template::exact_path_matches(&template.match_criteria, path);
        let relative_path_match = template::relative_path_matches(&template.match_criteria, path);
        let file_cols: std::collections::HashSet<&str> = state
            .source_schema()
            .iter_names()
            .map(|s| s.as_str())
            .collect();
        let exact_schema_match =
            template
                .match_criteria
                .schema_columns
                .as_ref()
                .is_some_and(|required| {
                    let required: std::collections::HashSet<&str> =
                        required.iter().map(|s| s.as_str()).collect();
                    required.is_subset(&file_cols) && file_cols.len() == required.len()
                });

        let mut details = format!("Total score: {:.1}\n\n", row.score);
        if exact_path_match && exact_schema_match {
            details.push_str("Exact path + exact schema: 2000.0\n");
        } else if exact_path_match {
            details.push_str("Exact path: 1000.0\n");
        } else if relative_path_match && exact_schema_match {
            details.push_str("Relative path + exact schema: 1950.0\n");
        } else if relative_path_match {
            details.push_str("Relative path: 950.0\n");
        } else if exact_schema_match {
            details.push_str("Exact schema: 900.0\n");
        } else {
            if template::path_pattern_matches(&template.match_criteria, path) {
                details.push_str("Path pattern match: 50.0+\n");
            }
            if template::filename_pattern_matches(&template.match_criteria, path) {
                details.push_str("Filename pattern match: 30.0+\n");
            }
            if let Some(required_cols) = &template.match_criteria.schema_columns {
                let matching_count = required_cols
                    .iter()
                    .filter(|col| file_cols.contains(col.as_str()))
                    .count();
                if matching_count > 0 {
                    details.push_str(&format!(
                        "Partial schema match: {:.1} ({} columns)\n",
                        matching_count as f64 * 2.0,
                        matching_count
                    ));
                }
            }
        }
        if template.usage_count > 0 {
            details.push_str(&format!(
                "Usage count: {:.1}\n",
                (template.usage_count.min(10) as f64) * 1.0
            ));
        }
        if let Some(last_used) = template.last_used
            && let Ok(duration) = std::time::SystemTime::now().duration_since(last_used)
        {
            let days_since = duration.as_secs() / 86400;
            if days_since <= 7 {
                details.push_str("Recent usage: 5.0\n");
            } else if days_since <= 30 {
                details.push_str("Recent usage: 2.0\n");
            }
        }
        Some((format!("Score: {}", template.name), details))
    }

    /// Set the appropriate help overlay visible (main, template, or analysis). No-op if already visible.
    fn open_help_overlay(&mut self) {
        let already = self.show_help
            || (self.template_modal.active && self.template_modal.show_help)
            || (self.analysis_modal.active && self.analysis_modal.show_help);
        if already {
            return;
        }
        if self.analysis_modal.active {
            self.analysis_modal.show_help = true;
        } else if self.template_modal.active {
            self.template_modal.show_help = true;
        } else {
            self.show_help = true;
        }
    }

    /// True while the confirmation modal is asking whether to download a remote file.
    ///
    /// That is the one confirmation the user has to be able to walk away from: the
    /// size probe behind it can take fifteen seconds, and the answer to "actually,
    /// never mind" is the home screen, not the exit.
    pub fn awaiting_download_confirmation(&self) -> bool {
        #[cfg(any(feature = "http", feature = "cloud"))]
        {
            self.confirmation_modal.active && self.pending_download.is_some()
        }
        #[cfg(not(any(feature = "http", feature = "cloud")))]
        {
            false
        }
    }

    fn key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        self.debug.on_key(event);

        // A completion flash lives until the next key: whatever this key does,
        // the bar's line about the last action is stale now.
        self.flash = None;

        let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
        // Ctrl-Q quits from anywhere, before any mode gets a say — including a mode
        // with no CONTROL arm of its own (the chart view) that would otherwise swallow
        // it while busy.
        if ctrl && event.code == KeyCode::Char('q') {
            return Some(AppEvent::Exit);
        }
        // Ctrl-C also quits from anywhere, except in a focused text field, where it is
        // the textarea's Copy binding and must reach it.
        if ctrl && event.code == KeyCode::Char('c') && !self.text_field_focused() {
            return Some(AppEvent::Exit);
        }

        if event.code == KeyCode::Esc
            && self.input_mode == InputMode::Normal
            && !self.analysis_modal.active
            && !self.error_modal.active
            && !self.confirmation_modal.active
            && self.return_from_quality_evidence(true)
        {
            return None;
        }

        // F1 opens help first so no other branch (e.g. Editing) can consume it.
        if event.code == KeyCode::F(1) {
            self.open_help_overlay();
            return None;
        }

        // Home owns the whole screen and every key while it is up — except under a
        // modal or the help overlay. Both render over home unconditionally, so if
        // home also ate their keys they would be undismissable, and Esc would try
        // to leave home instead.
        if self.input_mode == InputMode::Home
            && !self.confirmation_modal.active
            && !self.error_modal.active
            && !self.show_help
        {
            return self.home_key(event);
        }

        // Ctrl+O goes home from anywhere, including mid-load. That is what makes
        // browsing cheap: opening the wrong 300 MB file costs one keystroke to leave,
        // not a wait for it to finish.
        if event.code == KeyCode::Char('o')
            && event.modifiers.contains(KeyModifiers::CONTROL)
            && (!self.confirmation_modal.active || self.awaiting_download_confirmation())
        {
            self.enter_home();
            return None;
        }

        // Handle modals first - they have highest priority
        // Confirmation modal (for overwrite)
        if self.confirmation_modal.active {
            match event.code {
                KeyCode::Left | KeyCode::Char('h') => {
                    self.confirmation_modal.focus_yes = true;
                }
                KeyCode::Right | KeyCode::Char('l') => {
                    self.confirmation_modal.focus_yes = false;
                }
                KeyCode::Tab => {
                    // Toggle between Yes and No
                    self.confirmation_modal.focus_yes = !self.confirmation_modal.focus_yes;
                }
                // ←→ carry the choice, so ↑↓ scroll a long question; the
                // render clamps the offset.
                KeyCode::Up => {
                    self.confirmation_modal.scroll =
                        self.confirmation_modal.scroll.saturating_sub(1);
                }
                KeyCode::Down => {
                    self.confirmation_modal.scroll =
                        self.confirmation_modal.scroll.saturating_add(1);
                }
                KeyCode::Enter => {
                    if self.confirmation_modal.focus_yes {
                        // The confirmations that are not about overwriting a file come
                        // first: reading every row, and forgetting recents.
                        if std::mem::take(&mut self.pending_read_all) {
                            self.confirmation_modal.hide();
                            // Every row is a sample method like the others: it shows in
                            // the strip, and `s` changes it back.
                            let sample = sampling::Sample {
                                method: sampling::SampleMethod::EveryRow,
                                ..self.analysis_modal.sample.clone()
                            };
                            return self.apply_sample(sample);
                        }
                        if self.pending_clear_recents {
                            self.pending_clear_recents = false;
                            self.confirmation_modal.hide();
                            self.cache.clear_recents();
                            self.home_refresh();
                            self.home.status = None;
                            return None;
                        }
                        if let Some(place) = self.pending_forget_place.take() {
                            self.confirmation_modal.hide();
                            let paths = self.home.recents_in(&place);
                            self.cache.forget_recents(&paths);
                            self.home_refresh();
                            self.home.status = None;
                            return None;
                        }
                        // User confirmed overwrite: chart export first, then dataframe export
                        if let Some((path, format, title, width, height)) =
                            self.pending_chart_export.take()
                        {
                            self.confirmation_modal.hide();
                            return Some(AppEvent::ChartExport(path, format, title, width, height));
                        }
                        if let Some((path, format, options)) = self.pending_export.take() {
                            self.confirmation_modal.hide();
                            return Some(AppEvent::Export(path, format, options));
                        }
                        if let Some((format, header)) = self.pending_copy.take() {
                            self.confirmation_modal.hide();
                            return Some(AppEvent::CopyTable { format, header });
                        }
                        #[cfg(any(feature = "http", feature = "cloud"))]
                        if let Some((pending, lease)) = self.pending_download.take() {
                            // Dropped rather than held: the event returned below is a
                            // continuation, and the pump leases one of those. Dropping
                            // first is safe because a release is a queued event rather
                            // than a decrement — the count cannot dip between the two.
                            drop(lease);
                            self.confirmation_modal.hide();
                            if let LoadingState::Loading {
                                file_path,
                                file_size,
                                ..
                            } = &self.loading_state
                            {
                                self.loading_state = LoadingState::Loading {
                                    file_path: file_path.clone(),
                                    file_size: *file_size,
                                    current_phase: "Downloading".to_string(),
                                    progress_percent: 20,
                                };
                            }
                            return Some(match pending {
                                #[cfg(feature = "http")]
                                PendingDownload::Http { url, options, .. } => {
                                    AppEvent::DoDownloadHttp(url, options)
                                }
                                #[cfg(feature = "cloud")]
                                PendingDownload::S3 { url, options, .. } => {
                                    AppEvent::DoDownloadS3ToTemp(url, options)
                                }
                                #[cfg(feature = "cloud")]
                                PendingDownload::Gcs { url, options, .. }
                                | PendingDownload::Azure { url, options, .. } => {
                                    AppEvent::DoDownloadGcsToTemp(url, options)
                                }
                            });
                        }
                    } else {
                        self.pending_clear_recents = false;
                        self.pending_read_all = false;
                        self.pending_forget_place = None;
                        // Declining an overwrite returns to the filled form:
                        // the typed path, format and options survive the No.
                        if self.pending_chart_export.take().is_some() {
                            self.chart_export_modal.resume();
                        }
                        if self.pending_export.take().is_some() {
                            self.export_modal.resume();
                            self.input_mode = InputMode::Export;
                        }
                        self.pending_copy = None;
                        #[cfg(any(feature = "http", feature = "cloud"))]
                        if self.pending_download.is_some() {
                            self.enter_home();
                            return None;
                        }
                        self.confirmation_modal.hide();
                    }
                }
                KeyCode::Esc => {
                    // Disarmed on every exit from the modal, so a declined confirmation
                    // cannot fire against whatever the *next* one is asking about.
                    self.pending_clear_recents = false;
                    self.pending_read_all = false;
                    self.pending_forget_place = None;
                    // Declining an overwrite returns to the filled form: the
                    // typed path, format and options survive the Esc.
                    if self.pending_chart_export.take().is_some() {
                        self.chart_export_modal.resume();
                    }
                    if self.pending_export.take().is_some() {
                        self.export_modal.resume();
                        self.input_mode = InputMode::Export;
                    }
                    self.pending_copy = None;
                    #[cfg(any(feature = "http", feature = "cloud"))]
                    if self.pending_download.is_some() {
                        // Declining a download used to quit datui outright, which made
                        // a remote open the one thing in the app you could not back out
                        // of. `enter_home` clears the pending download and hides this.
                        self.enter_home();
                        return None;
                    }
                    self.confirmation_modal.hide();
                }
                _ => {}
            }
            return None;
        }
        // Error modal
        if self.error_modal.active {
            match event.code {
                // A long diagnostic scrolls; the render clamps the offset.
                KeyCode::Up | KeyCode::Char('k') => {
                    self.error_modal.scroll = self.error_modal.scroll.saturating_sub(1);
                    return None;
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.error_modal.scroll = self.error_modal.scroll.saturating_add(1);
                    return None;
                }
                KeyCode::PageUp => {
                    self.error_modal.scroll = self.error_modal.scroll.saturating_sub(8);
                    return None;
                }
                KeyCode::PageDown => {
                    self.error_modal.scroll = self.error_modal.scroll.saturating_add(8);
                    return None;
                }
                KeyCode::Esc | KeyCode::Enter => {
                    self.error_modal.hide();
                    // With nothing loaded, dismissing the error would otherwise leave
                    // an empty table and no indication of what to do. Go back to the
                    // list the dataset was chosen from, carrying the reason, so the
                    // next choice is one keystroke away.
                    if self.data_table_state.is_none() {
                        let reason = self.last_load_error.take();
                        self.enter_home();
                        self.home.status = reason;
                    }
                }
                _ => {}
            }
            return None;
        }

        // Main table: left/right scroll columns (before help/mode blocks so column scroll always works in Normal).
        // No is_press()/is_release() check: some terminals do not report key kind correctly.
        // Exclude template/analysis modals so they can handle Left/Right themselves.
        let in_main_table = !(self.input_mode != InputMode::Normal
            || self.show_help
            || self.template_modal.active
            || self.analysis_modal.active);
        if in_main_table {
            let did_scroll = match event.code {
                KeyCode::Right | KeyCode::Char('l') => {
                    if let Some(ref mut state) = self.data_table_state {
                        state.scroll_right();
                        if self.debug.enabled {
                            self.debug.last_action = "scroll_right".to_string();
                        }
                        true
                    } else {
                        false
                    }
                }
                KeyCode::Left | KeyCode::Char('h') => {
                    if let Some(ref mut state) = self.data_table_state {
                        state.scroll_left();
                        if self.debug.enabled {
                            self.debug.last_action = "scroll_left".to_string();
                        }
                        true
                    } else {
                        false
                    }
                }
                _ => false,
            };
            if did_scroll {
                return None;
            }
        }

        if self.show_help
            || (self.template_modal.active && self.template_modal.show_help)
            || (self.analysis_modal.active && self.analysis_modal.show_help)
        {
            match event.code {
                KeyCode::Esc => {
                    if self.analysis_modal.active && self.analysis_modal.show_help {
                        self.analysis_modal.show_help = false;
                    } else if self.template_modal.active && self.template_modal.show_help {
                        self.template_modal.show_help = false;
                    } else {
                        self.show_help = false;
                    }
                    self.help_scroll = 0;
                }
                KeyCode::Char('?') => {
                    if self.analysis_modal.active && self.analysis_modal.show_help {
                        self.analysis_modal.show_help = false;
                    } else if self.template_modal.active && self.template_modal.show_help {
                        self.template_modal.show_help = false;
                    } else {
                        self.show_help = false;
                    }
                    self.help_scroll = 0;
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.help_scroll = self.help_scroll.saturating_add(1);
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.help_scroll = self.help_scroll.saturating_sub(1);
                }
                KeyCode::PageDown => {
                    self.help_scroll = self.help_scroll.saturating_add(10);
                }
                KeyCode::PageUp => {
                    self.help_scroll = self.help_scroll.saturating_sub(10);
                }
                KeyCode::Home => {
                    self.help_scroll = 0;
                }
                KeyCode::End => {
                    // The render clamps this to the last page and persists the result.
                    self.help_scroll = usize::MAX;
                }
                _ => {}
            }
            return None;
        }

        if event.code == KeyCode::Char('?') {
            let ctrl_help = event.modifiers.contains(KeyModifiers::CONTROL);
            // The home screen always accepts characters, into its filter or path input.
            let in_text_input = self.text_field_focused() || self.input_mode == InputMode::Home;
            // Ctrl-? always opens help; bare ? only when not in a text field
            if ctrl_help || !in_text_input {
                self.open_help_overlay();
                return None;
            }
        }

        if self.input_mode == InputMode::SortFilter {
            let on_tab_bar = self.sort_filter_modal.focus == SortFilterFocus::TabBar;
            let on_body = self.sort_filter_modal.focus == SortFilterFocus::Body;
            let sort_tab = self.sort_filter_modal.active_tab == SortFilterTab::Sort;
            let filter_tab = self.sort_filter_modal.active_tab == SortFilterTab::Filter;
            let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
            let on_find =
                on_body && sort_tab && self.sort_filter_modal.sort.focus == SortFocus::Filter;
            let on_column_list =
                on_body && sort_tab && self.sort_filter_modal.sort.focus == SortFocus::ColumnList;
            // The status line is about the last key; this one replaces it.
            self.sort_filter_modal.sort.status = None;

            // Ctrl+J is the apply chord beside Ctrl+Enter: it works on every
            // terminal, and some send Ctrl+Enter as Ctrl+J.
            let apply_chord = ctrl && matches!(event.code, KeyCode::Enter | KeyCode::Char('j'));

            // The inline filter editor owns the keys while it is up: a small form
            // within the form. Esc ends the edit and only the edit.
            if filter_tab && self.sort_filter_modal.filter.editor.is_some() {
                if apply_chord {
                    return self.apply_sort_filter();
                }
                let m = &mut self.sort_filter_modal.filter;
                let editor = m.editor.as_mut().expect("checked above");
                match event.code {
                    KeyCode::Esc => m.cancel_editor(),
                    // Enter chooses the step's pick; from the value it commits the row.
                    // Space chooses too: a space typed into the narrowing filter
                    // matches nothing and blanks the list. (The value field below
                    // keeps Space for typing.)
                    KeyCode::Enter | KeyCode::Tab | KeyCode::Right | KeyCode::Char(' ')
                        if editor.step != FilterEditStep::Value =>
                    {
                        match editor.step {
                            FilterEditStep::Column => {
                                if editor.column.selected_original().is_some() {
                                    editor.step = FilterEditStep::Operator;
                                }
                            }
                            FilterEditStep::Operator => {
                                editor.step = FilterEditStep::Value;
                                // Pre-filled from the statement under edit; typing
                                // replaces it, arrows keep it editable.
                                editor.value.select_all();
                            }
                            FilterEditStep::Value => {}
                        }
                    }
                    KeyCode::Enter => m.commit_editor(),
                    KeyCode::BackTab => {
                        editor.step = match editor.step {
                            FilterEditStep::Column | FilterEditStep::Operator => {
                                FilterEditStep::Column
                            }
                            FilterEditStep::Value => FilterEditStep::Operator,
                        };
                    }
                    KeyCode::Up => match editor.step {
                        FilterEditStep::Column => editor.column.move_up(),
                        FilterEditStep::Operator => editor.operator.move_up(),
                        FilterEditStep::Value => {}
                    },
                    KeyCode::Down => match editor.step {
                        FilterEditStep::Column => editor.column.move_down(),
                        FilterEditStep::Operator => editor.operator.move_down(),
                        FilterEditStep::Value => {}
                    },
                    KeyCode::Backspace if editor.step == FilterEditStep::Column => {
                        editor.column.backspace();
                    }
                    KeyCode::Backspace if editor.step == FilterEditStep::Operator => {
                        editor.operator.backspace();
                    }
                    KeyCode::Char(c) if editor.step == FilterEditStep::Column => {
                        editor.column.filter_key(c, event.modifiers);
                    }
                    KeyCode::Char(c) if editor.step == FilterEditStep::Operator => {
                        editor.operator.filter_key(c, event.modifiers);
                    }
                    // The value is an ordinary text field, readline included.
                    _ if editor.step == FilterEditStep::Value => {
                        let _ = editor.value.handle_key(event, None);
                    }
                    _ => {}
                }
                return None;
            }

            match event.code {
                KeyCode::Esc => {
                    for col in &mut self.sort_filter_modal.sort.columns {
                        col.is_to_be_locked = false;
                    }
                    self.sort_filter_modal.sort.has_unapplied_changes = false;
                    self.sort_filter_modal.close();
                    self.input_mode = InputMode::Normal;
                }
                _ if apply_chord => return self.apply_sort_filter(),
                KeyCode::Tab => self.sort_filter_modal.next_focus(),
                KeyCode::BackTab => self.sort_filter_modal.prev_focus(),
                // The find field keeps its readline keys; Up/Down and the rest fall
                // through to the arms below.
                _ if on_find
                    && !matches!(
                        event.code,
                        KeyCode::Tab
                            | KeyCode::BackTab
                            | KeyCode::Esc
                            | KeyCode::Enter
                            | KeyCode::Up
                            | KeyCode::Down
                    ) =>
                {
                    let _ = self
                        .sort_filter_modal
                        .sort
                        .filter_input
                        .handle_key(event, Some(&self.cache));
                }
                // Arrows switch tabs from the tab bar and from the lists; only a text
                // field keeps them to itself.
                KeyCode::Left | KeyCode::Right if on_tab_bar || on_body => {
                    self.sort_filter_modal.switch_tab();
                }
                KeyCode::Char('h') | KeyCode::Char('l') if on_tab_bar => {
                    self.sort_filter_modal.switch_tab();
                }
                // On the Filters list Enter edits the row under the cursor (or starts
                // a new one on the add row); everywhere else Enter applies.
                // On the Filters tab Enter means add/edit wherever focus sits — the
                // sidebar opens on the tab bar, and Enter closing the dialog from
                // there is how a first filter never gets added. The footer says
                // ^J is the apply here.
                KeyCode::Enter if filter_tab => {
                    self.sort_filter_modal.focus = SortFilterFocus::Body;
                    let history_limit = self.history_limit;
                    self.sort_filter_modal
                        .filter
                        .open_editor(&self.theme, history_limit);
                }
                KeyCode::Enter => return self.apply_sort_filter(),
                // Enter means add/edit on this tab, so apply gets a key that needs
                // no modifier: Ctrl+Enter only exists on terminals speaking the
                // kitty protocol.
                KeyCode::Char('a') if filter_tab => return self.apply_sort_filter(),
                // Columns list: every per-column property, one key each.
                KeyCode::Char(' ') if on_column_list => {
                    self.sort_filter_modal.sort.cycle_sort();
                }
                KeyCode::Up | KeyCode::Char('k') if on_body && sort_tab => {
                    let s = &mut self.sort_filter_modal.sort;
                    if s.focus == SortFocus::ColumnList {
                        let i = match s.table_state.selected() {
                            Some(i) => {
                                if i == 0 {
                                    s.filtered_columns().len().saturating_sub(1)
                                } else {
                                    i - 1
                                }
                            }
                            None => 0,
                        };
                        s.table_state.select(Some(i));
                    }
                }
                KeyCode::Down | KeyCode::Char('j') if on_body && sort_tab => {
                    let s = &mut self.sort_filter_modal.sort;
                    if s.focus == SortFocus::ColumnList {
                        let i = match s.table_state.selected() {
                            Some(i) => {
                                if i >= s.filtered_columns().len().saturating_sub(1) {
                                    0
                                } else {
                                    i + 1
                                }
                            }
                            None => 0,
                        };
                        s.table_state.select(Some(i));
                    } else {
                        s.focus = SortFocus::ColumnList;
                    }
                }
                KeyCode::Char(']') if on_column_list => {
                    self.sort_filter_modal.sort.move_selection_down();
                }
                KeyCode::Char('[') if on_column_list => {
                    self.sort_filter_modal.sort.move_selection_up();
                }
                KeyCode::Char('+') | KeyCode::Char('=') if on_column_list => {
                    self.sort_filter_modal.sort.move_column_display_up();
                    self.sort_filter_modal.sort.has_unapplied_changes = true;
                }
                KeyCode::Char('-') | KeyCode::Char('_') if on_column_list => {
                    self.sort_filter_modal.sort.move_column_display_down();
                    self.sort_filter_modal.sort.has_unapplied_changes = true;
                }
                KeyCode::Char('L') if on_column_list => {
                    self.sort_filter_modal.sort.toggle_lock_at_column();
                    self.sort_filter_modal.sort.has_unapplied_changes = true;
                }
                KeyCode::Char('v') if on_column_list => {
                    self.sort_filter_modal.sort.toggle_visibility();
                    self.sort_filter_modal.sort.has_unapplied_changes = true;
                }
                KeyCode::Char('C') if on_body && sort_tab => {
                    self.sort_filter_modal.sort.clear_selection();
                }
                KeyCode::Char(c) if on_column_list && c.is_ascii_digit() => {
                    if let Some(digit) = c.to_digit(10) {
                        self.sort_filter_modal
                            .sort
                            .jump_selection_to_order(digit as usize);
                    }
                }
                // Filters list: the cursor walks the statements plus the add row.
                KeyCode::Up | KeyCode::Char('k') if on_body && filter_tab => {
                    self.sort_filter_modal.filter.move_cursor_up();
                }
                KeyCode::Down | KeyCode::Char('j') if on_body && filter_tab => {
                    self.sort_filter_modal.filter.move_cursor_down();
                }
                KeyCode::Char('d') | KeyCode::Delete if on_body && filter_tab => {
                    self.sort_filter_modal.filter.delete_at_cursor();
                }
                KeyCode::Delete if on_column_list => {
                    self.sort_filter_modal.sort.remove_sort();
                }
                KeyCode::Char(' ') if on_body && filter_tab => {
                    self.sort_filter_modal.filter.toggle_logical_at_cursor();
                }
                KeyCode::Char('C') if on_body && filter_tab => {
                    self.sort_filter_modal.filter.statements.clear();
                    self.sort_filter_modal.filter.cursor = 0;
                }
                _ => {}
            }
            return None;
        }

        if self.input_mode == InputMode::Export {
            match event.code {
                KeyCode::Esc => {
                    self.export_modal.close();
                    self.input_mode = InputMode::Normal;
                }
                KeyCode::Tab => self.export_modal.next_focus(),
                KeyCode::BackTab => self.export_modal.prev_focus(),
                KeyCode::Up | KeyCode::Char('k') => {
                    match self.export_modal.focus {
                        ExportFocus::FormatSelector => {
                            // Cycle through formats
                            let current_idx = ExportFormat::ALL
                                .iter()
                                .position(|&f| f == self.export_modal.selected_format)
                                .unwrap_or(0);
                            let prev_idx = if current_idx == 0 {
                                ExportFormat::ALL.len() - 1
                            } else {
                                current_idx - 1
                            };
                            self.export_modal.selected_format = ExportFormat::ALL[prev_idx];
                            self.export_modal.sync_path_to_format();
                        }
                        ExportFocus::PathInput => {
                            // Pass to text input widget (for history navigation)
                            self.export_path_key(event);
                        }
                        ExportFocus::CsvDelimiter => {
                            // Pass to text input widget (for history navigation)
                            self.export_modal
                                .csv_delimiter_input
                                .handle_key(event, None);
                        }
                        ExportFocus::CsvCompression
                        | ExportFocus::JsonCompression
                        | ExportFocus::NdjsonCompression => {
                            // Left to move to previous compression option
                            self.export_modal.cycle_compression_backward();
                        }
                        _ => {
                            self.export_modal.prev_focus();
                        }
                    }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    match self.export_modal.focus {
                        ExportFocus::FormatSelector => {
                            // Cycle through formats
                            let current_idx = ExportFormat::ALL
                                .iter()
                                .position(|&f| f == self.export_modal.selected_format)
                                .unwrap_or(0);
                            let next_idx = (current_idx + 1) % ExportFormat::ALL.len();
                            self.export_modal.selected_format = ExportFormat::ALL[next_idx];
                            self.export_modal.sync_path_to_format();
                        }
                        ExportFocus::PathInput => {
                            // Pass to text input widget (for history navigation)
                            self.export_path_key(event);
                        }
                        ExportFocus::CsvDelimiter => {
                            // Pass to text input widget (for history navigation)
                            self.export_modal
                                .csv_delimiter_input
                                .handle_key(event, None);
                        }
                        ExportFocus::CsvCompression
                        | ExportFocus::JsonCompression
                        | ExportFocus::NdjsonCompression => {
                            // Right to move to next compression option
                            self.export_modal.cycle_compression();
                        }
                        _ => {
                            self.export_modal.next_focus();
                        }
                    }
                }
                KeyCode::Left | KeyCode::Char('h') => {
                    match self.export_modal.focus {
                        ExportFocus::PathInput => {
                            self.export_path_key(event);
                        }
                        ExportFocus::CsvDelimiter => {
                            self.export_modal
                                .csv_delimiter_input
                                .handle_key(event, None);
                        }
                        ExportFocus::FormatSelector => {
                            // Don't change focus in format selector
                        }
                        ExportFocus::CsvCompression
                        | ExportFocus::JsonCompression
                        | ExportFocus::NdjsonCompression => {
                            // Move to previous compression option
                            self.export_modal.cycle_compression_backward();
                        }
                        _ => self.export_modal.prev_focus(),
                    }
                }
                KeyCode::Right | KeyCode::Char('l') => {
                    match self.export_modal.focus {
                        ExportFocus::PathInput => {
                            self.export_path_key(event);
                        }
                        ExportFocus::CsvDelimiter => {
                            self.export_modal
                                .csv_delimiter_input
                                .handle_key(event, None);
                        }
                        ExportFocus::FormatSelector => {
                            // Don't change focus in format selector
                        }
                        ExportFocus::CsvCompression
                        | ExportFocus::JsonCompression
                        | ExportFocus::NdjsonCompression => {
                            // Move to next compression option
                            self.export_modal.cycle_compression();
                        }
                        _ => self.export_modal.next_focus(),
                    }
                }
                KeyCode::Enter => {
                    // Enter applies from anywhere in the form: build the export from
                    // the state every row already echoes. A blank path cannot, and
                    // says so inline instead of doing nothing.
                    let path_str = self.export_modal.path_input.value().trim().to_string();
                    if path_str.is_empty() {
                        self.export_modal.path_error = Some("Enter a file path.");
                        self.export_modal.focus = ExportFocus::PathInput;
                    }
                    if !path_str.is_empty() {
                        // `~` and `$VAR` expand as everywhere else a path is
                        // typed; unexpanded they become a literal `~` directory
                        // or a NotFound from the writer.
                        let mut path = home::expand_user_path(&path_str);
                        let format = self.export_modal.selected_format;
                        let compression = match format {
                            ExportFormat::Csv => self.export_modal.csv_compression,
                            ExportFormat::Json => self.export_modal.json_compression,
                            ExportFormat::Ndjson => self.export_modal.ndjson_compression,
                            ExportFormat::Parquet | ExportFormat::Ipc | ExportFormat::Avro => None,
                        };
                        // Ensure file extension is present (including compression extension if needed)
                        let path_with_ext = Self::ensure_file_extension(&path, format, compression);
                        // Update the path input to show the extension
                        if path_with_ext != path {
                            self.export_modal
                                .path_input
                                .set_value(path_with_ext.display().to_string());
                        }
                        path = path_with_ext;
                        let delimiter = self
                            .export_modal
                            .csv_delimiter_input
                            .value()
                            .chars()
                            .next()
                            .unwrap_or(',') as u8;
                        let options = ExportOptions {
                            csv_delimiter: delimiter,
                            csv_include_header: self.export_modal.csv_include_header,
                            csv_compression: self.export_modal.csv_compression,
                            json_compression: self.export_modal.json_compression,
                            ndjson_compression: self.export_modal.ndjson_compression,
                            parquet_compression: None,
                            source_file: self.export_modal.source_file,
                        };
                        // Check if file exists and show confirmation
                        if path.exists() {
                            let path_display = path.display().to_string();
                            self.pending_export = Some((path, format, options));
                            self.confirmation_modal.show_destructive(
                                format!("File already exists:\n{path_display}\n\nOverwrite it?"),
                                "Overwrite",
                            );
                            // Suspended, not closed: declining returns to the
                            // filled form with the typed path intact.
                            self.export_modal.suspend();
                            self.input_mode = InputMode::Normal;
                        } else {
                            // Start export with progress
                            self.export_modal.close();
                            self.input_mode = InputMode::Normal;
                            return Some(AppEvent::Export(path, format, options));
                        }
                    }
                }
                KeyCode::Char(' ') => {
                    // Space to toggle checkboxes, but pass to text inputs if they're focused
                    match self.export_modal.focus {
                        ExportFocus::PathInput => {
                            // Pass spacebar to text input
                            self.export_path_key(event);
                        }
                        ExportFocus::CsvDelimiter => {
                            // Pass spacebar to text input
                            self.export_modal
                                .csv_delimiter_input
                                .handle_key(event, None);
                        }
                        ExportFocus::CsvIncludeHeader => {
                            // Toggle checkbox
                            self.export_modal.csv_include_header =
                                !self.export_modal.csv_include_header;
                        }
                        ExportFocus::SourceFile => {
                            self.export_modal.source_file = !self.export_modal.source_file;
                        }
                        _ => {}
                    }
                }
                KeyCode::Char(_)
                | KeyCode::Backspace
                | KeyCode::Delete
                | KeyCode::Home
                | KeyCode::End => {
                    match self.export_modal.focus {
                        ExportFocus::PathInput => {
                            self.export_path_key(event);
                        }
                        ExportFocus::CsvDelimiter => {
                            self.export_modal
                                .csv_delimiter_input
                                .handle_key(event, None);
                        }
                        ExportFocus::FormatSelector => {
                            // Don't input text in format selector
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
            return None;
        }

        if self.input_mode == InputMode::Copy {
            let picker_open = self.copy_modal.picker.is_some();
            if event.code == KeyCode::Char('?') && !picker_open {
                self.show_help = true;
                return None;
            }

            // The open Picker owns the keys: type to narrow, ↑↓ move, Enter
            // chooses, and Esc backs out of the Picker and only the Picker.
            if picker_open {
                match event.code {
                    KeyCode::Esc => self.copy_modal.picker = None,
                    KeyCode::Enter => self.copy_modal.picker_choose(),
                    KeyCode::Tab => {
                        self.copy_modal.picker_choose();
                        self.copy_modal.next_focus();
                    }
                    KeyCode::BackTab => {
                        self.copy_modal.picker_choose();
                        self.copy_modal.prev_focus();
                    }
                    KeyCode::Up => {
                        if let Some(picker) = self.copy_modal.picker.as_mut() {
                            picker.move_up();
                        }
                    }
                    KeyCode::Down => {
                        if let Some(picker) = self.copy_modal.picker.as_mut() {
                            picker.move_down();
                        }
                    }
                    // Every row here picks one, so Space chooses like Enter; a
                    // typed space would narrow the list to nothing.
                    KeyCode::Char(' ') => self.copy_modal.picker_choose(),
                    KeyCode::Backspace => {
                        if let Some(picker) = self.copy_modal.picker.as_mut() {
                            picker.backspace();
                        }
                    }
                    KeyCode::Char(c) => {
                        if let Some(picker) = self.copy_modal.picker.as_mut() {
                            picker.filter_key(c, event.modifiers);
                        }
                    }
                    _ => {}
                }
                return None;
            }

            // Whatever this key does, the form is being edited again: the
            // re-accented gap line goes back to plain (Enter below re-arms it).
            self.copy_modal.attention = false;

            match event.code {
                KeyCode::Esc => {
                    self.copy_modal.close();
                    self.input_mode = InputMode::Normal;
                }
                // Enter copies from anywhere in the form; what it will do has
                // been echoed on the spec line all along.
                KeyCode::Enter => {
                    if self.copy_modal.validation_error().is_some() {
                        self.copy_modal.attention = true;
                        return None;
                    }
                    return self.perform_copy();
                }
                KeyCode::Tab | KeyCode::Down | KeyCode::Char('j') => self.copy_modal.next_focus(),
                KeyCode::BackTab | KeyCode::Up | KeyCode::Char('k') => self.copy_modal.prev_focus(),
                KeyCode::Char(' ') => {
                    if self.copy_modal.focus == copy_modal::CopyFocus::Header {
                        self.copy_modal.toggle_header();
                    } else {
                        self.copy_modal.open_picker();
                    }
                }
                _ => {}
            }
            return None;
        }

        if self.input_mode == InputMode::PivotMelt {
            let picker_open = self.pivot_melt_modal.picker.is_some();
            let text_focus = !picker_open
                && self
                    .pivot_melt_modal
                    .is_text_row(self.pivot_melt_modal.focus);
            let ctrl_help = event.modifiers.contains(KeyModifiers::CONTROL);
            if event.code == KeyCode::Char('?') && (ctrl_help || (!text_focus && !picker_open)) {
                self.show_help = true;
                return None;
            }

            // The open Picker owns the keys: type to narrow, ↑↓ move, Space
            // toggles on a several-choice row, Enter chooses, and Esc backs
            // out of the Picker and only the Picker.
            if picker_open {
                match event.code {
                    KeyCode::Esc => self.pivot_melt_modal.picker = None,
                    KeyCode::Enter => self.pivot_melt_modal.picker_choose(),
                    KeyCode::Tab => {
                        self.pivot_melt_modal.picker_choose();
                        self.pivot_melt_modal.next_focus();
                    }
                    KeyCode::BackTab => {
                        self.pivot_melt_modal.picker_choose();
                        self.pivot_melt_modal.prev_focus();
                    }
                    KeyCode::Up => {
                        if let Some(picker) = self.pivot_melt_modal.picker.as_mut() {
                            picker.move_up();
                        }
                    }
                    KeyCode::Down => {
                        if let Some(picker) = self.pivot_melt_modal.picker.as_mut() {
                            picker.move_down();
                        }
                    }
                    KeyCode::Char(' ')
                        if self
                            .pivot_melt_modal
                            .is_multi_row(self.pivot_melt_modal.focus) =>
                    {
                        self.pivot_melt_modal.picker_toggle();
                    }
                    // On a pick-one row Space chooses like Enter. It must
                    // never reach the narrowing filter: a typed space matches
                    // nothing, and the list blanking under the key that just
                    // opened it reads as breakage.
                    KeyCode::Char(' ') => self.pivot_melt_modal.picker_choose(),
                    KeyCode::Backspace => {
                        if let Some(picker) = self.pivot_melt_modal.picker.as_mut() {
                            picker.backspace();
                        }
                    }
                    KeyCode::Char(c) => {
                        if let Some(picker) = self.pivot_melt_modal.picker.as_mut() {
                            picker.filter_key(c, event.modifiers);
                        }
                    }
                    _ => {}
                }
                return None;
            }

            // Whatever this key does, the form is being edited again: the
            // re-accented gap line goes back to plain (Enter below re-arms it).
            self.pivot_melt_modal.attention = false;

            match event.code {
                KeyCode::Esc => {
                    self.pivot_melt_modal.close();
                    self.input_mode = InputMode::Normal;
                }
                // Enter applies from anywhere in the form; what it will do has
                // been echoed on the spec line all along.
                KeyCode::Enter => {
                    return match self.pivot_melt_modal.active_tab {
                        PivotMeltTab::Pivot => {
                            if self.pivot_melt_modal.pivot_validation_error().is_some() {
                                // The spec line already names the gap; it
                                // re-accents rather than a modal repeating it.
                                self.pivot_melt_modal.attention = true;
                                None
                            } else {
                                self.pivot_melt_modal
                                    .build_pivot_spec()
                                    .map(AppEvent::Pivot)
                            }
                        }
                        PivotMeltTab::Melt => {
                            if self.pivot_melt_modal.melt_validation_error().is_some() {
                                self.pivot_melt_modal.attention = true;
                                None
                            } else {
                                self.pivot_melt_modal.build_melt_spec().map(AppEvent::Melt)
                            }
                        }
                    };
                }
                KeyCode::Tab | KeyCode::Down => self.pivot_melt_modal.next_focus(),
                KeyCode::BackTab | KeyCode::Up => self.pivot_melt_modal.prev_focus(),
                // Arrows switch tabs from the tab bar and the picked rows; a
                // text row keeps them for its cursor.
                KeyCode::Left | KeyCode::Right if !text_focus => {
                    self.pivot_melt_modal.switch_tab();
                }
                KeyCode::Char('h') | KeyCode::Char('l')
                    if self.pivot_melt_modal.focus == PivotMeltFocus::TabBar =>
                {
                    self.pivot_melt_modal.switch_tab();
                }
                // A picked row edits through the Picker scoped to that row
                // alone: Space opens it, typing opens it already narrowed.
                KeyCode::Char(' ')
                    if self
                        .pivot_melt_modal
                        .is_picker_row(self.pivot_melt_modal.focus) =>
                {
                    self.pivot_melt_modal.open_picker();
                }
                KeyCode::Char(c)
                    if self
                        .pivot_melt_modal
                        .is_picker_row(self.pivot_melt_modal.focus) =>
                {
                    // Only a plain character opens the picker by typing;
                    // a chord is a chord, not the first letter of a search.
                    if event
                        .modifiers
                        .intersection(KeyModifiers::CONTROL | KeyModifiers::ALT)
                        .is_empty()
                    {
                        self.pivot_melt_modal.open_picker();
                        if let Some(picker) = self.pivot_melt_modal.picker.as_mut() {
                            picker.type_char(c);
                        }
                    }
                }
                // A text row is an ordinary text field, readline included.
                _ if text_focus => {
                    if let Some(input) = self.pivot_melt_modal.focused_text_input_mut() {
                        let _ = input.handle_key(event, None);
                    }
                }
                _ => {}
            }
            return None;
        }

        if self.input_mode == InputMode::Info {
            let on_body = self.info_modal.focus == InfoFocus::Body;
            let schema_tab = self.info_modal.active_tab == InfoTab::Schema;
            let notes_tab = self.info_modal.active_tab == InfoTab::Notes;
            let notes = self
                .data_table_state
                .as_ref()
                .map(|s| s.notes().len())
                .unwrap_or(0);
            let total_rows = self
                .data_table_state
                .as_ref()
                .map(|s| s.schema.len())
                .unwrap_or(0);
            let visible = self.info_modal.schema_visible_height;

            match event.code {
                KeyCode::Esc | KeyCode::Char('i') if event.is_press() => {
                    self.info_modal.close();
                    self.input_mode = InputMode::Normal;
                }
                KeyCode::Tab if event.is_press() && schema_tab => {
                    self.info_modal.next_focus();
                }
                KeyCode::BackTab if event.is_press() && schema_tab => {
                    self.info_modal.prev_focus();
                }
                // From anywhere in the panel: the arrows have no other job on any tab's
                // body, and the panel opens with the body focused, so gating them on
                // tab-bar focus made a fresh `i` then `→` do nothing.
                KeyCode::Left | KeyCode::Char('h') if event.is_press() => {
                    let (has_partitions, has_notes) = self.info_tabs_on_offer();
                    self.info_modal.switch_tab_prev(has_partitions, has_notes);
                }
                KeyCode::Right | KeyCode::Char('l') if event.is_press() => {
                    let (has_partitions, has_notes) = self.info_tabs_on_offer();
                    self.info_modal.switch_tab(has_partitions, has_notes);
                }
                KeyCode::Down | KeyCode::Char('j') if event.is_press() && on_body && schema_tab => {
                    self.info_modal.schema_table_down(total_rows, visible);
                }
                KeyCode::Up | KeyCode::Char('k') if event.is_press() && on_body && schema_tab => {
                    self.info_modal.schema_table_up(total_rows, visible);
                }
                KeyCode::Down | KeyCode::Char('j') if event.is_press() && notes_tab => {
                    self.info_modal.notes_move(1, notes);
                }
                KeyCode::Up | KeyCode::Char('k') if event.is_press() && notes_tab => {
                    self.info_modal.notes_move(-1, notes);
                }
                KeyCode::Enter if event.is_press() && notes_tab => {
                    self.read_the_selected_note_s_column_as_text();
                }
                _ => {}
            }
            return None;
        }

        if self.input_mode == InputMode::Chart {
            // Chart export modal (sub-dialog within Chart mode)
            if self.chart_export_modal.active {
                match event.code {
                    KeyCode::Esc if event.is_press() => {
                        self.chart_export_modal.close();
                    }
                    KeyCode::Tab if event.is_press() => {
                        self.chart_export_modal.next_focus();
                    }
                    KeyCode::BackTab if event.is_press() => {
                        self.chart_export_modal.prev_focus();
                    }
                    KeyCode::Up | KeyCode::Char('k')
                        if event.is_press()
                            && self.chart_export_modal.focus
                                == ChartExportFocus::FormatSelector =>
                    {
                        let idx = ChartExportFormat::ALL
                            .iter()
                            .position(|&f| f == self.chart_export_modal.selected_format)
                            .unwrap_or(0);
                        let prev = if idx == 0 {
                            ChartExportFormat::ALL.len() - 1
                        } else {
                            idx - 1
                        };
                        self.chart_export_modal.selected_format = ChartExportFormat::ALL[prev];
                    }
                    KeyCode::Down | KeyCode::Char('j')
                        if event.is_press()
                            && self.chart_export_modal.focus
                                == ChartExportFocus::FormatSelector =>
                    {
                        let idx = ChartExportFormat::ALL
                            .iter()
                            .position(|&f| f == self.chart_export_modal.selected_format)
                            .unwrap_or(0);
                        let next = (idx + 1) % ChartExportFormat::ALL.len();
                        self.chart_export_modal.selected_format = ChartExportFormat::ALL[next];
                    }
                    // Enter applies from anywhere in the form: build the
                    // export from the state every row already echoes. A blank
                    // path exports nothing.
                    KeyCode::Enter if event.is_press() => {
                        let path_str = self.chart_export_modal.path_input.value().trim();
                        if !path_str.is_empty() {
                            let title = self
                                .chart_export_modal
                                .title_input
                                .value()
                                .trim()
                                .to_string();
                            let (width, height) = self.chart_export_modal.export_dimensions();
                            // `~` and `$VAR` expand as everywhere else a path
                            // is typed; unexpanded, the PNG/EPS writer fails
                            // with NotFound on the literal `~` directory.
                            let mut path = home::expand_user_path(path_str);
                            let format = self.chart_export_modal.selected_format;
                            // Only add default extension when user did not provide one
                            if path.extension().is_none() {
                                path.set_extension(format.extension());
                            }
                            let path_display = path.display().to_string();
                            if path.exists() {
                                self.pending_chart_export =
                                    Some((path, format, title, width, height));
                                // Suspended, not closed: declining returns to
                                // the filled form with the typed path intact.
                                self.chart_export_modal.suspend();
                                self.confirmation_modal.show_destructive(
                                    format!(
                                        "File already exists:\n{path_display}\n\nOverwrite it?"
                                    ),
                                    "Overwrite",
                                );
                            } else {
                                self.chart_export_modal.close();
                                return Some(AppEvent::ChartExport(
                                    path, format, title, width, height,
                                ));
                            }
                        }
                    }
                    _ => {
                        if event.is_press() {
                            if self.chart_export_modal.focus == ChartExportFocus::TitleInput {
                                let _ = self.chart_export_modal.title_input.handle_key(event, None);
                            } else if self.chart_export_modal.focus == ChartExportFocus::PathInput {
                                let _ = self.chart_export_modal.path_input.handle_key(event, None);
                            } else if self.chart_export_modal.focus == ChartExportFocus::WidthInput
                            {
                                let allow = match event.code {
                                    KeyCode::Char(c) if c.is_ascii_digit() => true,
                                    KeyCode::Backspace
                                    | KeyCode::Delete
                                    | KeyCode::Left
                                    | KeyCode::Right
                                    | KeyCode::Home
                                    | KeyCode::End => true,
                                    _ => false,
                                };
                                if allow {
                                    let _ =
                                        self.chart_export_modal.width_input.handle_key(event, None);
                                }
                            } else if self.chart_export_modal.focus == ChartExportFocus::HeightInput
                            {
                                let allow = match event.code {
                                    KeyCode::Char(c) if c.is_ascii_digit() => true,
                                    KeyCode::Backspace
                                    | KeyCode::Delete
                                    | KeyCode::Left
                                    | KeyCode::Right
                                    | KeyCode::Home
                                    | KeyCode::End => true,
                                    _ => false,
                                };
                                if allow {
                                    let _ = self
                                        .chart_export_modal
                                        .height_input
                                        .handle_key(event, None);
                                }
                            }
                        }
                    }
                }
                return None;
            }

            // The open Picker owns the keys: type to narrow, ↑↓ move, Space
            // toggles on the Y series row, Enter chooses, and Esc backs out
            // of the Picker and only the Picker.
            if self.chart_modal.picker.is_some() {
                match event.code {
                    KeyCode::Esc if event.is_press() => self.chart_modal.picker = None,
                    KeyCode::Enter if event.is_press() => self.chart_modal.picker_choose(),
                    KeyCode::Tab if event.is_press() => {
                        self.chart_modal.picker_choose();
                        self.chart_modal.next_focus();
                    }
                    KeyCode::BackTab if event.is_press() => {
                        self.chart_modal.picker_choose();
                        self.chart_modal.prev_focus();
                    }
                    KeyCode::Up if event.is_press() => {
                        if let Some(picker) = self.chart_modal.picker.as_mut() {
                            picker.move_up();
                        }
                    }
                    KeyCode::Down if event.is_press() => {
                        if let Some(picker) = self.chart_modal.picker.as_mut() {
                            picker.move_down();
                        }
                    }
                    KeyCode::Char(' ')
                        if event.is_press()
                            && self.chart_modal.is_multi_row(self.chart_modal.focus) =>
                    {
                        self.chart_modal.picker_toggle();
                    }
                    // On a pick-one row Space chooses like Enter — what Space
                    // always did on these lists. It must never reach the
                    // narrowing filter: a typed space matches nothing, and
                    // the list blanking under the key that just opened it
                    // reads as breakage.
                    KeyCode::Char(' ') if event.is_press() => {
                        self.chart_modal.picker_choose();
                    }
                    KeyCode::Backspace if event.is_press() => {
                        if let Some(picker) = self.chart_modal.picker.as_mut() {
                            picker.backspace();
                        }
                    }
                    KeyCode::Char(c) if event.is_press() => {
                        if let Some(picker) = self.chart_modal.picker.as_mut() {
                            picker.filter_key(c, event.modifiers);
                        }
                    }
                    _ => {}
                }
                return None;
            }

            match event.code {
                // The chart kind switches from anywhere: 1-5 name a tab in
                // order, [ and ] cycle. Safe as plain keys — with the Picker
                // closed, nothing on this screen types.
                KeyCode::Char(c @ '1'..='5') if event.is_press() => {
                    let idx = c as usize - '1' as usize;
                    self.chart_modal.set_chart_kind(ChartKind::ALL[idx]);
                }
                KeyCode::Char('[') if event.is_press() => {
                    self.chart_modal.prev_chart_kind();
                }
                KeyCode::Char(']') if event.is_press() => {
                    self.chart_modal.next_chart_kind();
                }
                KeyCode::Char('e') if event.is_press() => {
                    // Open chart export modal when there is something visible to export
                    if self.data_table_state.is_some() && self.chart_modal.can_export() {
                        self.chart_export_modal
                            .open(&self.theme, self.history_limit);
                    }
                }
                // q/Q do nothing in chart view (no exit)
                KeyCode::Char('?') if event.is_press() => {
                    self.show_help = true;
                }
                KeyCode::Esc if event.is_press() => {
                    self.chart_modal.close();
                    self.reset_chart_state();
                    self.input_mode = InputMode::Normal;
                }
                KeyCode::Tab if event.is_press() => {
                    self.chart_modal.next_focus();
                }
                KeyCode::BackTab if event.is_press() => {
                    self.chart_modal.prev_focus();
                }
                // Enter or Space edits the focused row: a column row opens
                // its Picker, a toggle flips, the style cycles.
                KeyCode::Enter | KeyCode::Char(' ') if event.is_press() => {
                    match self.chart_modal.focus {
                        ChartFocus::YStartsAtZero => self.chart_modal.toggle_y_starts_at_zero(),
                        ChartFocus::LogScale => self.chart_modal.toggle_log_scale(),
                        ChartFocus::ShowLegend => self.chart_modal.toggle_show_legend(),
                        ChartFocus::Style => self.chart_modal.next_chart_type(),
                        focus if self.chart_modal.is_picker_row(focus) => {
                            self.chart_modal.open_picker();
                        }
                        _ => {}
                    }
                }
                KeyCode::Char('+') | KeyCode::Char('=') if event.is_press() => {
                    self.chart_modal.adjust_number_row(1);
                }
                KeyCode::Char('-') if event.is_press() => {
                    self.chart_modal.adjust_number_row(-1);
                }
                KeyCode::Left | KeyCode::Char('h') if event.is_press() => {
                    match self.chart_modal.focus {
                        ChartFocus::Style => self.chart_modal.prev_chart_type(),
                        _ => self.chart_modal.adjust_number_row(-1),
                    }
                }
                KeyCode::Right | KeyCode::Char('l') if event.is_press() => {
                    match self.chart_modal.focus {
                        ChartFocus::Style => self.chart_modal.next_chart_type(),
                        _ => self.chart_modal.adjust_number_row(1),
                    }
                }
                KeyCode::PageUp if event.is_press() => {
                    if self.chart_modal.focus == ChartFocus::LimitRows {
                        self.chart_modal.adjust_row_limit_page(1);
                    }
                }
                KeyCode::PageDown if event.is_press() => {
                    if self.chart_modal.focus == ChartFocus::LimitRows {
                        self.chart_modal.adjust_row_limit_page(-1);
                    }
                }
                KeyCode::Up | KeyCode::Char('k') if event.is_press() => {
                    self.chart_modal.prev_focus();
                }
                KeyCode::Down | KeyCode::Char('j') if event.is_press() => {
                    self.chart_modal.next_focus();
                }
                _ => {}
            }
            return None;
        }

        if self.analysis_modal.active {
            // A run in flight, whichever tool: Esc stops waiting for it. It acts at once
            // (see `hard_escape_while_busy`) rather than queueing behind the run it is
            // meant to cancel.
            if event.code == KeyCode::Esc && self.analysis_modal.computing.is_some() {
                self.cancel_analysis();
                return None;
            }
            // The Sample form owns the keys while it has the cursor: always when it
            // floats over a result, and in a tool's empty pane once Tab moves in.
            if self
                .analysis_modal
                .sample_form
                .as_ref()
                .is_some_and(|form| {
                    !form.inline || self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main
                })
            {
                return self.sample_form_key(event);
            }
            if event.code == KeyCode::Char('s') && self.analysis_modal.sample_key_opens_form() {
                self.open_sample_form();
                return None;
            }
            if event.code == KeyCode::Char('v') && self.analysis_modal.sample_key_opens_form() {
                return self.read_sample_view();
            }
            if self.analysis_modal.selected_tool == Some(analysis_modal::AnalysisTool::DataQuality)
                && self.analysis_modal.view == analysis_modal::AnalysisView::Main
                // Before the first run the pane is the Sample form, not the plan.
                && self.analysis_modal.sample_form.is_none()
            {
                use crate::data_quality::QualityPage;

                // A finding's popup scrolls when it holds more than the screen does.
                if self.analysis_modal.data_quality_observation_detail {
                    let rows = match event.code {
                        KeyCode::Down | KeyCode::Char('j') => Some(1),
                        KeyCode::Up | KeyCode::Char('k') => Some(-1),
                        KeyCode::PageDown => Some(10),
                        KeyCode::PageUp => Some(-10),
                        KeyCode::End => Some(i32::from(u16::MAX)),
                        KeyCode::Home => Some(-i32::from(u16::MAX)),
                        _ => None,
                    };
                    if let Some(rows) = rows {
                        self.analysis_modal.scroll_quality_detail(rows);
                        return None;
                    }
                }
                if (self.analysis_modal.data_quality_confirm_run
                    || self.analysis_modal.data_quality_show_access
                    || self.analysis_modal.data_quality_observation_detail)
                    && !matches!(event.code, KeyCode::Esc | KeyCode::Enter)
                {
                    return None;
                }

                // A plan field's choices own the keys while they are open.
                if self.analysis_modal.data_quality_picker.is_some() {
                    match event.code {
                        KeyCode::Esc => self.analysis_modal.data_quality_picker = None,
                        KeyCode::Enter => self.analysis_modal.choose_plan_picker(),
                        code => {
                            if let Some(picker) = self.analysis_modal.data_quality_picker.as_mut() {
                                match code {
                                    KeyCode::Up => picker.state.move_up(),
                                    KeyCode::Down => picker.state.move_down(),
                                    KeyCode::Backspace => picker.state.backspace(),
                                    KeyCode::Char(c) => picker.state.filter_key(c, event.modifiers),
                                    _ => {}
                                }
                            }
                        }
                    }
                    return None;
                }
                // The role editor owns the keys: the role, and its column.
                if self.analysis_modal.data_quality_page == QualityPage::TimeRoles
                    && event.code != KeyCode::Char('?')
                {
                    let field = self.analysis_modal.data_quality_plan_field;
                    match event.code {
                        KeyCode::Esc | KeyCode::Enter => {
                            if event.code == KeyCode::Esc
                                && let Some(plan) =
                                    self.analysis_modal.data_quality_plan_before_edit.take()
                            {
                                self.analysis_modal.data_quality_plan = plan;
                            }
                            self.analysis_modal.data_quality_plan_before_edit = None;
                            self.analysis_modal.set_quality_page(QualityPage::Plan);
                            self.analysis_modal.data_quality_plan_field = 4;
                        }
                        KeyCode::Up | KeyCode::Char('k') => {
                            self.analysis_modal.data_quality_plan_field = field.saturating_sub(1);
                        }
                        KeyCode::Down | KeyCode::Char('j') => {
                            self.analysis_modal.data_quality_plan_field =
                                (field + 1).min(crate::data_quality::TemporalRole::ALL.len() - 1);
                        }
                        KeyCode::Left
                        | KeyCode::Char('h')
                        | KeyCode::Right
                        | KeyCode::Char('l') => {
                            let columns = self
                                .data_table_state
                                .as_ref()
                                .map(|state| {
                                    state.quality_temporal_columns(
                                        &self.analysis_modal.data_quality_plan.scope,
                                    )
                                })
                                .unwrap_or_default();
                            self.analysis_modal.cycle_quality_time_role(
                                field,
                                &columns,
                                matches!(event.code, KeyCode::Right | KeyCode::Char('l')),
                            );
                        }
                        _ => {}
                    }
                    return None;
                }
                // The plan is edited where it stands: ↑↓ the field, Space its choices,
                // Enter runs from any field, Esc puts back what the last run used.
                if self.analysis_modal.data_quality_page == QualityPage::Plan
                    && self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main
                {
                    let rows = self.analysis_modal.quality_plan_rows();
                    let field = self.analysis_modal.data_quality_plan_field.min(rows - 1);
                    match event.code {
                        KeyCode::Up | KeyCode::Char('k') => {
                            self.analysis_modal.data_quality_plan_field = field.saturating_sub(1);
                            return None;
                        }
                        KeyCode::Down | KeyCode::Char('j') => {
                            self.analysis_modal.data_quality_plan_field = (field + 1).min(rows - 1);
                            return None;
                        }
                        KeyCode::Char(' ') => {
                            self.analysis_modal.data_quality_plan_field = field;
                            return self.open_plan_field();
                        }
                        KeyCode::Esc if self.analysis_modal.quality_plan_pending() => {
                            if let Some(plan) = self.analysis_modal.data_quality_last_plan.clone() {
                                self.analysis_modal.data_quality_plan = plan;
                            }
                            return None;
                        }
                        _ => {}
                    }
                }

                match event.code {
                    KeyCode::Esc if self.analysis_modal.data_quality_show_access => {
                        self.analysis_modal.data_quality_show_access = false;
                        return None;
                    }
                    KeyCode::Esc if self.analysis_modal.data_quality_observation_detail => {
                        self.analysis_modal.data_quality_observation_detail = false;
                        return None;
                    }
                    KeyCode::Enter if self.analysis_modal.data_quality_show_access => {
                        self.analysis_modal.data_quality_show_access = false;
                        return None;
                    }
                    KeyCode::Enter if self.analysis_modal.data_quality_observation_detail => {
                        // The clean entry has no rows to open; Enter shows every
                        // check it passed, and again the most important few.
                        if self.analysis_modal.quality_selected_is_clean() {
                            self.analysis_modal.data_quality_checks_expanded =
                                !self.analysis_modal.data_quality_checks_expanded;
                            self.analysis_modal.data_quality_detail_scroll.offset = 0;
                            return None;
                        }
                        let event = self.open_quality_evidence();
                        if self.analysis_modal.active && !self.error_modal.active {
                            self.analysis_modal.data_quality_observation_detail = false;
                        }
                        return event;
                    }
                    KeyCode::Esc if self.analysis_modal.data_quality_confirm_run => {
                        self.analysis_modal.data_quality_confirm_run = false;
                        return None;
                    }
                    // A drill-in backs out to the list it came from.
                    KeyCode::Esc
                        if self.analysis_modal.data_quality_page == QualityPage::SegmentDetail =>
                    {
                        self.analysis_modal.close_segment_detail();
                        return None;
                    }
                    KeyCode::Esc
                        if self.analysis_modal.data_quality_page == QualityPage::Detail =>
                    {
                        self.analysis_modal
                            .set_quality_column_page(QualityPage::Columns);
                        return None;
                    }
                    KeyCode::Esc
                        if !matches!(self.analysis_modal.data_quality_page, QualityPage::Plan) =>
                    {
                        self.analysis_modal.set_quality_page(QualityPage::Plan);
                        // The plan page acts on Enter (run) and only the main pane
                        // hears it; opening the page brings the cursor along.
                        self.analysis_modal.focus = analysis_modal::AnalysisFocus::Main;
                        return None;
                    }
                    KeyCode::Char('p') => {
                        self.analysis_modal.data_quality_show_access =
                            !self.analysis_modal.data_quality_show_access;
                        return None;
                    }
                    // The plan is a tab like the others; e goes there from anywhere.
                    KeyCode::Char('e') => {
                        self.analysis_modal.show_quality_tab(QualityPage::Plan);
                        return None;
                    }
                    KeyCode::Char(digit @ '1'..='4') => {
                        let tab = digit as usize - '1' as usize;
                        self.analysis_modal.show_quality_tab(QualityPage::TABS[tab]);
                        return None;
                    }
                    KeyCode::Char('m')
                        if self.analysis_modal.data_quality_page == QualityPage::Trends =>
                    {
                        self.analysis_modal.cycle_quality_metric();
                        return None;
                    }
                    KeyCode::Char('b')
                        if self.analysis_modal.data_quality_page == QualityPage::Segments
                            && self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
                    {
                        // The segment under the cursor, looked up while the results
                        // still order the list.
                        let selected = self.analysis_modal.selected_segment();
                        if let Some(mut results) = self.analysis_modal.data_quality_results.take() {
                            if let Some(label) = selected
                                .and_then(|index| results.segments.get(index))
                                .map(|segment| segment.label.clone())
                            {
                                let mut plan = self.analysis_modal.quality_result_plan().clone();
                                plan.comparison = crate::data_quality::QualityComparison::Baseline;
                                plan.baseline_segment = Some(label.clone());
                                results.compare_segments(&plan);
                                self.cache_quality_result(&results, plan.clone());
                                self.analysis_modal.data_quality_last_plan = Some(plan);
                                let working = &mut self.analysis_modal.data_quality_plan;
                                working.comparison =
                                    crate::data_quality::QualityComparison::Baseline;
                                working.baseline_segment = Some(label);
                            }
                            self.analysis_modal.data_quality_results = Some(results);
                        }
                        return None;
                    }
                    KeyCode::Char('o')
                        if self.analysis_modal.data_quality_page == QualityPage::Segments =>
                    {
                        self.analysis_modal.toggle_segment_order();
                        return None;
                    }
                    // Another sample for every tool: the seed is the shared sample's.
                    KeyCode::Char('r') => {
                        let sample = sampling::Sample {
                            seed: sample_modal::new_seed(),
                            ..self.analysis_modal.sample.clone()
                        };
                        return self.apply_sample(sample);
                    }
                    KeyCode::Enter
                        if self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
                    {
                        if self.analysis_modal.data_quality_page == QualityPage::Plan {
                            self.sync_quality_plan();
                            if self.analysis_modal.data_quality_results.is_some()
                                && self.analysis_modal.data_quality_last_plan.as_ref()
                                    == Some(&self.analysis_modal.data_quality_plan)
                            {
                                self.analysis_modal.set_quality_page(QualityPage::Overview);
                                return None;
                            }
                            if self.restore_cached_quality() {
                                return None;
                            }
                            if self
                                .analysis_modal
                                .data_quality_plan
                                .requires_confirmation()
                                && !self.analysis_modal.data_quality_confirm_run
                            {
                                self.analysis_modal.data_quality_confirm_run = true;
                                return None;
                            }
                            self.analysis_modal.data_quality_confirm_run = false;
                            self.analysis_modal.data_quality_results = None;
                            self.analysis_modal.data_quality_from_cache = false;
                            self.analysis_modal.computing =
                                Some(AnalysisProgress::new("Profiling data quality"));
                            self.busy = true;
                            return Some(AppEvent::AnalysisDataQualityCompute);
                        } else if let Some(setup) = self.quality_page_setup() {
                            // Straight to the setting that fills the page, on the plan.
                            self.analysis_modal.show_quality_tab(QualityPage::Plan);
                            self.analysis_modal.data_quality_plan_field = match setup {
                                data_quality::QualitySetup::Grain => 1,
                                data_quality::QualitySetup::TimeRoles => 4,
                            };
                            return self.open_plan_field();
                        } else if self.analysis_modal.data_quality_page == QualityPage::Overview {
                            let findings = self.analysis_modal.quality_row_count();
                            self.analysis_modal.data_quality_checks_expanded = false;
                            self.analysis_modal.data_quality_detail_scroll =
                                analysis_modal::DetailScroll::default();
                            self.analysis_modal.data_quality_observation_detail = self
                                .analysis_modal
                                .data_quality_table_state
                                .selected()
                                .is_some_and(|index| index < findings);
                        } else if self.analysis_modal.data_quality_page == QualityPage::Columns {
                            self.analysis_modal
                                .set_quality_column_page(QualityPage::Detail);
                        } else if self.analysis_modal.data_quality_page == QualityPage::Detail {
                            self.analysis_modal
                                .set_quality_column_page(QualityPage::Columns);
                        } else if self.analysis_modal.data_quality_page == QualityPage::Segments
                            && self.analysis_modal.data_quality_results.is_some()
                        {
                            self.analysis_modal.open_segment_detail();
                        } else if self.analysis_modal.data_quality_page
                            == QualityPage::SegmentDetail
                        {
                            self.analysis_modal.close_segment_detail();
                        }
                        return None;
                    }
                    KeyCode::Down | KeyCode::Char('j')
                        if self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
                    {
                        let rows = self.analysis_modal.quality_row_count();
                        self.analysis_modal.next_row(rows);
                        return None;
                    }
                    KeyCode::Up | KeyCode::Char('k')
                        if self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
                    {
                        self.analysis_modal.previous_row();
                        return None;
                    }
                    KeyCode::Left | KeyCode::Char('h') => {
                        self.analysis_modal.step_quality_tab(false);
                        return None;
                    }
                    KeyCode::Right | KeyCode::Char('l') => {
                        self.analysis_modal.step_quality_tab(true);
                        return None;
                    }
                    KeyCode::PageDown
                        if self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
                    {
                        let rows = self.analysis_modal.quality_row_count();
                        self.analysis_modal.page_down(rows, 10);
                        return None;
                    }
                    KeyCode::PageUp
                        if self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
                    {
                        self.analysis_modal.page_up(10);
                        return None;
                    }
                    KeyCode::Home
                        if self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
                    {
                        self.analysis_modal.data_quality_table_state.select(Some(0));
                        return None;
                    }
                    KeyCode::End
                        if self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
                    {
                        let rows = self.analysis_modal.quality_row_count();
                        if rows > 0 {
                            self.analysis_modal
                                .data_quality_table_state
                                .select(Some(rows - 1));
                        }
                        return None;
                    }
                    _ => {}
                }
            }
            match event.code {
                KeyCode::Esc => {
                    if self.analysis_modal.show_help {
                        self.analysis_modal.show_help = false;
                    } else if self.analysis_modal.view != analysis_modal::AnalysisView::Main {
                        // Close detail view
                        self.analysis_modal.close_detail();
                    } else {
                        self.analysis_modal.close();
                    }
                }
                KeyCode::Char('?') => {
                    self.analysis_modal.show_help = !self.analysis_modal.show_help;
                }
                // Another sample, or every row. Both only where the results are a
                // sample, and only on the main view: inside a detail an undocumented
                // `r` cleared the results out from under it.
                KeyCode::Char('r') if self.analysis_results_are_sampled() => {
                    let sample = sampling::Sample {
                        seed: sample_modal::new_seed(),
                        ..self.analysis_modal.sample.clone()
                    };
                    return self.apply_sample(sample);
                }
                // Refused while a cancelled run is still reading: Polars cannot stop it,
                // and a second full read beside it is how memory runs out.
                KeyCode::Char('a')
                    if self.analysis_results_are_sampled() && self.cancelled_work_running() =>
                {
                    self.flash_note("A cancelled run is still finishing; try again shortly".into());
                }
                KeyCode::Char('a') if self.analysis_results_are_sampled() => {
                    let total = self
                        .analysis_modal
                        .current_results()
                        .map(|r| r.total_rows)
                        .unwrap_or_default();
                    self.pending_read_all = true;
                    self.confirmation_modal.show(format!(
                        "Read all {} rows? It can take much longer than the sample. \
                         Esc stops waiting; the read finishes in the background.",
                        crate::numfmt::group_chrome(total)
                    ));
                    self.confirmation_modal.yes_label = "Read all";
                }
                KeyCode::Tab => {
                    // One rule for the whole screen: Tab moves sidebar <-> result.
                    // The detail views have a single focusable thing, so it stays.
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main {
                        self.analysis_modal.switch_focus();
                        self.sync_sample_form_focus();
                    }
                }
                KeyCode::Enter
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main =>
                {
                    if self.analysis_modal.focus == analysis_modal::AnalysisFocus::Sidebar {
                        // Enter again on the tool whose Sample form is showing runs it
                        // with the form as it stands: two Enters from the list take the
                        // defaults, and the cursor never leaves it.
                        if self.analysis_modal.sample_form.as_ref().is_some_and(|f| f.inline)
                            && self.analysis_modal.highlighted_tool()
                                == self.analysis_modal.selected_tool
                        {
                            return self.run_sample_form();
                        }
                        // Select tool from sidebar
                        self.analysis_modal.select_tool();
                        self.analysis_modal.sample_form = None;
                        // A tool with a result shows it. One without shows the Sample
                        // form in its pane, so the first run reads the rows asked for;
                        // Enter runs it with the defaults as they stand.
                        let has_result = match self.analysis_modal.selected_tool {
                            Some(analysis_modal::AnalysisTool::Describe) => {
                                self.analysis_modal.describe_results.is_some()
                            }
                            Some(analysis_modal::AnalysisTool::DistributionAnalysis) => {
                                self.analysis_modal.distribution_results.is_some()
                            }
                            Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                                self.analysis_modal.correlation_results.is_some()
                            }
                            Some(analysis_modal::AnalysisTool::DataQuality) => {
                                self.restore_recent_quality_plan();
                                // The plan's rows are the shared sample's, whatever
                                // the last plan here read.
                                self.sync_quality_plan();
                                self.restore_cached_quality()
                                    || self.analysis_modal.data_quality_results.is_some()
                            }
                            None => true,
                        };
                        // Once a sample has been run on this dataset, every tool reads
                        // it: a tool with no result runs at once, and s changes the
                        // sample for all of them.
                        let sample_run = self.analysis_modal.sample_run_for
                            == Some(self.dataset_generation);
                        if !has_result && sample_run {
                            return match self.analysis_modal.selected_tool {
                                Some(analysis_modal::AnalysisTool::DataQuality) => {
                                    self.run_quality_plan()
                                }
                                _ => self.start_analysis_run(),
                            };
                        }
                        // Before the first, the form is what the pane is for, so the
                        // cursor goes with it: Enter runs, the arrows change a setting,
                        // Esc hands the cursor back to the list. A tool with a result
                        // leaves the cursor on the list.
                        if !has_result {
                            self.analysis_modal.focus = analysis_modal::AnalysisFocus::Main;
                            self.open_first_run_form();
                        }
                    } else {
                        // Enter in main area opens detail view if applicable
                        match self.analysis_modal.selected_tool {
                            Some(analysis_modal::AnalysisTool::DistributionAnalysis) => {
                                self.analysis_modal.open_distribution_detail();
                            }
                            Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                                self.analysis_modal.open_correlation_detail();
                            }
                            _ => {}
                        }
                    }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    match self.analysis_modal.view {
                        analysis_modal::AnalysisView::Main => {
                            match self.analysis_modal.focus {
                                analysis_modal::AnalysisFocus::Sidebar => {
                                    // Navigate sidebar tool list
                                    self.analysis_modal.next_tool();
                                }
                                analysis_modal::AnalysisFocus::Main => {
                                    // Navigate in main area based on selected tool
                                    match self.analysis_modal.selected_tool {
                                        Some(analysis_modal::AnalysisTool::Describe) => {
                                            if let Some(state) = &self.data_table_state {
                                                let max_rows = state.schema.len();
                                                self.analysis_modal.next_row(max_rows);
                                            }
                                        }
                                        Some(
                                            analysis_modal::AnalysisTool::DistributionAnalysis,
                                        ) => {
                                            if let Some(results) =
                                                self.analysis_modal.current_results()
                                            {
                                                let max_rows = results.distribution_analyses.len();
                                                self.analysis_modal.next_row(max_rows);
                                            }
                                        }
                                        Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                                            if let Some(results) =
                                                self.analysis_modal.current_results()
                                                && let Some(corr) = &results.correlation_matrix {
                                                    let max_rows = corr.columns.len();
                                                    // Calculate visible columns (same logic as horizontal moves)
                                                    let row_header_width = 20u16;
                                                    let cell_width = 12u16;
                                                    let column_spacing = 1u16;
                                                    let estimated_width = 80u16;
                                                    let available_width = estimated_width
                                                        .saturating_sub(row_header_width);
                                                    let mut calculated_visible = 0usize;
                                                    let mut used = 0u16;
                                                    let max_cols = corr.columns.len();
                                                    loop {
                                                        let needed = if calculated_visible == 0 {
                                                            cell_width
                                                        } else {
                                                            column_spacing + cell_width
                                                        };
                                                        if used + needed <= available_width
                                                            && calculated_visible < max_cols
                                                        {
                                                            used += needed;
                                                            calculated_visible += 1;
                                                        } else {
                                                            break;
                                                        }
                                                    }
                                                    let visible_cols =
                                                        calculated_visible.max(1).min(max_cols);
                                                    self.analysis_modal.move_correlation_cell(
                                                        (1, 0),
                                                        max_rows,
                                                        max_rows,
                                                        visible_cols,
                                                    );
                                                }
                                        }
                                        Some(analysis_modal::AnalysisTool::DataQuality) => {}
                                        None => {}
                                    }
                                }
                                _ => {}
                            }
                        }
                        analysis_modal::AnalysisView::DistributionDetail
                            if self.analysis_modal.focus
                                == analysis_modal::AnalysisFocus::DistributionSelector =>
                        {
                            self.analysis_modal.next_distribution();
                        }
                        _ => {}
                    }
                }
                KeyCode::Char('s')
                    // Toggle histogram scale (linear/log) in distribution detail view
                    if self.analysis_modal.view
                        == analysis_modal::AnalysisView::DistributionDetail =>
                {
                    self.analysis_modal.histogram_scale = match self.analysis_modal.histogram_scale {
                        analysis_modal::HistogramScale::Linear => analysis_modal::HistogramScale::Log,
                        analysis_modal::HistogramScale::Log => analysis_modal::HistogramScale::Linear,
                    };
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main {
                        self.analysis_modal.previous_row();
                    } else if self.analysis_modal.view
                        == analysis_modal::AnalysisView::DistributionDetail
                        && self.analysis_modal.focus
                            == analysis_modal::AnalysisFocus::DistributionSelector
                    {
                        self.analysis_modal.previous_distribution();
                    }
                }
                KeyCode::Left | KeyCode::Char('h')
                    if !event.modifiers.contains(KeyModifiers::CONTROL)
                        && self.analysis_modal.view == analysis_modal::AnalysisView::Main =>
                {
                    match self.analysis_modal.focus {
                        analysis_modal::AnalysisFocus::Sidebar => {
                            // Sidebar navigation handled by Up/Down
                        }
                        analysis_modal::AnalysisFocus::DistributionSelector => {
                            // Distribution selector navigation handled by Up/Down
                        }
                        analysis_modal::AnalysisFocus::Main => {
                            match self.analysis_modal.selected_tool {
                                Some(analysis_modal::AnalysisTool::Describe) => {
                                    self.analysis_modal.scroll_left();
                                }
                                Some(analysis_modal::AnalysisTool::DistributionAnalysis) => {
                                    self.analysis_modal.scroll_left();
                                }
                                Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                                    if let Some(results) = self.analysis_modal.current_results()
                                        && let Some(corr) = &results.correlation_matrix {
                                            let max_cols = corr.columns.len();
                                            // Calculate visible columns using same logic as render function
                                            // This matches the render_correlation_matrix calculation
                                            let row_header_width = 20u16;
                                            let cell_width = 12u16;
                                            let column_spacing = 1u16;
                                            // Use a conservative estimate for available width
                                            // In practice, main_area.width would be available, but we don't have access here
                                            // Using a reasonable default that works for most terminals
                                            let estimated_width = 80u16; // Conservative estimate (most terminals are 80+ wide)
                                            let available_width =
                                                estimated_width.saturating_sub(row_header_width);
                                            // Match render logic: first column has no spacing, subsequent ones do
                                            let mut calculated_visible = 0usize;
                                            let mut used = 0u16;
                                            loop {
                                                let needed = if calculated_visible == 0 {
                                                    cell_width
                                                } else {
                                                    column_spacing + cell_width
                                                };
                                                if used + needed <= available_width
                                                    && calculated_visible < max_cols
                                                {
                                                    used += needed;
                                                    calculated_visible += 1;
                                                } else {
                                                    break;
                                                }
                                            }
                                            let visible_cols =
                                                calculated_visible.max(1).min(max_cols);
                                            self.analysis_modal.move_correlation_cell(
                                                (0, -1),
                                                max_cols,
                                                max_cols,
                                                visible_cols,
                                            );
                                        }
                                }
                                Some(analysis_modal::AnalysisTool::DataQuality) => {}
                                None => {}
                            }
                        }
                    }
                }
                KeyCode::Right | KeyCode::Char('l')
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main =>
                {
                    match self.analysis_modal.focus {
                        analysis_modal::AnalysisFocus::Sidebar => {
                            // Sidebar navigation handled by Up/Down
                        }
                        analysis_modal::AnalysisFocus::DistributionSelector => {
                            // Distribution selector navigation handled by Up/Down
                        }
                        analysis_modal::AnalysisFocus::Main => {
                            match self.analysis_modal.selected_tool {
                                Some(analysis_modal::AnalysisTool::Describe) => {
                                    // Number of statistics: count, null_count, mean, std, min, 25%, 50%, 75%, max, skewness, kurtosis, distribution
                                    let max_stats = 12;
                                    // Estimate visible stats based on terminal width (rough estimate)
                                    let visible_stats = 8; // Will be calculated more accurately in widget
                                    self.analysis_modal.scroll_right(max_stats, visible_stats);
                                }
                                Some(analysis_modal::AnalysisTool::DistributionAnalysis) => {
                                    // Number of statistics: Distribution, P-value, Shapiro-Wilk, SW p-value, CV, Outliers, Skewness, Kurtosis
                                    let max_stats = 8;
                                    // Estimate visible stats based on terminal width (rough estimate)
                                    let visible_stats = 6; // Will be calculated more accurately in widget
                                    self.analysis_modal.scroll_right(max_stats, visible_stats);
                                }
                                Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                                    if let Some(results) = self.analysis_modal.current_results()
                                        && let Some(corr) = &results.correlation_matrix {
                                            let max_cols = corr.columns.len();
                                            // Calculate visible columns using same logic as render function
                                            let row_header_width = 20u16;
                                            let cell_width = 12u16;
                                            let column_spacing = 1u16;
                                            let estimated_width = 80u16; // Conservative estimate
                                            let available_width =
                                                estimated_width.saturating_sub(row_header_width);
                                            let mut calculated_visible = 0usize;
                                            let mut used = 0u16;
                                            loop {
                                                let needed = if calculated_visible == 0 {
                                                    cell_width
                                                } else {
                                                    column_spacing + cell_width
                                                };
                                                if used + needed <= available_width
                                                    && calculated_visible < max_cols
                                                {
                                                    used += needed;
                                                    calculated_visible += 1;
                                                } else {
                                                    break;
                                                }
                                            }
                                            let visible_cols =
                                                calculated_visible.max(1).min(max_cols);
                                            self.analysis_modal.move_correlation_cell(
                                                (0, 1),
                                                max_cols,
                                                max_cols,
                                                visible_cols,
                                            );
                                        }
                                }
                                Some(analysis_modal::AnalysisTool::DataQuality) => {}
                                None => {}
                            }
                        }
                    }
                }
                KeyCode::PageDown
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main
                        && self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
                {
                    match self.analysis_modal.selected_tool {
                        Some(analysis_modal::AnalysisTool::Describe) => {
                            if let Some(state) = &self.data_table_state {
                                let max_rows = state.schema.len();
                                let page_size = 10;
                                self.analysis_modal.page_down(max_rows, page_size);
                            }
                        }
                        Some(analysis_modal::AnalysisTool::DistributionAnalysis) => {
                            if let Some(results) = self.analysis_modal.current_results() {
                                let max_rows = results.distribution_analyses.len();
                                let page_size = 10;
                                self.analysis_modal.page_down(max_rows, page_size);
                            }
                        }
                        Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                            if let Some(results) = self.analysis_modal.current_results()
                                && let Some(corr) = &results.correlation_matrix {
                                    let max_rows = corr.columns.len();
                                    let page_size = 10;
                                    self.analysis_modal.page_down(max_rows, page_size);
                                }
                        }
                        Some(analysis_modal::AnalysisTool::DataQuality) => {}
                        None => {}
                    }
                }
                KeyCode::PageUp
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main
                        && self.analysis_modal.focus == analysis_modal::AnalysisFocus::Main =>
                {
                    let page_size = 10;
                    self.analysis_modal.page_up(page_size);
                }
                KeyCode::Home
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main =>
                {
                    match self.analysis_modal.focus {
                        analysis_modal::AnalysisFocus::Sidebar => {
                            self.analysis_modal.sidebar_state.select(Some(0));
                        }
                        analysis_modal::AnalysisFocus::DistributionSelector => {
                            self.analysis_modal
                                .distribution_selector_state
                                .select(Some(0));
                        }
                        analysis_modal::AnalysisFocus::Main => {
                            match self.analysis_modal.selected_tool {
                                Some(analysis_modal::AnalysisTool::Describe) => {
                                    self.analysis_modal.table_state.select(Some(0));
                                }
                                Some(analysis_modal::AnalysisTool::DistributionAnalysis) => {
                                    self.analysis_modal
                                        .distribution_table_state
                                        .select(Some(0));
                                }
                                Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                                    self.analysis_modal.correlation_table_state.select(Some(0));
                                    self.analysis_modal.selected_correlation = Some((0, 0));
                                }
                                Some(analysis_modal::AnalysisTool::DataQuality) => {
                                    self.analysis_modal.data_quality_table_state.select(Some(0));
                                }
                                None => {}
                            }
                        }
                    }
                }
                KeyCode::End
                    if self.analysis_modal.view == analysis_modal::AnalysisView::Main =>
                {
                    match self.analysis_modal.focus {
                        analysis_modal::AnalysisFocus::Sidebar => {
                            self.analysis_modal.sidebar_state.select(Some(3));
                            // Last tool
                        }
                        analysis_modal::AnalysisFocus::DistributionSelector => {
                            self.analysis_modal
                                .distribution_selector_state
                                .select(Some(13)); // Last distribution (Weibull, index 13 of 14 total)
                        }
                        analysis_modal::AnalysisFocus::Main => {
                            match self.analysis_modal.selected_tool {
                                Some(analysis_modal::AnalysisTool::Describe) => {
                                    if let Some(state) = &self.data_table_state {
                                        let max_rows = state.schema.len();
                                        if max_rows > 0 {
                                            self.analysis_modal
                                                .table_state
                                                .select(Some(max_rows - 1));
                                        }
                                    }
                                }
                                Some(analysis_modal::AnalysisTool::DistributionAnalysis) => {
                                    if let Some(results) = self.analysis_modal.current_results() {
                                        let max_rows = results.distribution_analyses.len();
                                        if max_rows > 0 {
                                            self.analysis_modal
                                                .distribution_table_state
                                                .select(Some(max_rows - 1));
                                        }
                                    }
                                }
                                Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                                    if let Some(results) = self.analysis_modal.current_results()
                                        && let Some(corr) = &results.correlation_matrix {
                                            let max_rows = corr.columns.len();
                                            if max_rows > 0 {
                                                self.analysis_modal
                                                    .correlation_table_state
                                                    .select(Some(max_rows - 1));
                                                self.analysis_modal.selected_correlation =
                                                    Some((max_rows - 1, max_rows - 1));
                                            }
                                        }
                                }
                                Some(analysis_modal::AnalysisTool::DataQuality) => {
                                    let rows = self.analysis_modal.quality_row_count();
                                    if rows > 0 {
                                        self.analysis_modal
                                            .data_quality_table_state
                                            .select(Some(rows - 1));
                                    }
                                }
                                None => {}
                            }
                        }
                    }
                }
                _ => {}
            }
            return None;
        }

        if self.template_modal.active {
            let form = self.template_modal.mode != TemplateModalMode::List;
            let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
            // The list's status line is about the last key; this one replaces it.
            self.template_modal.status = None;
            match event.code {
                KeyCode::Esc => {
                    if self.template_modal.score_details.is_some() {
                        self.template_modal.score_details = None;
                    } else if self.template_modal.delete_confirm {
                        self.template_modal.delete_confirm = false;
                    } else if self.template_modal.show_help {
                        self.template_modal.show_help = false;
                    } else if form {
                        // Back to the list; the form's staged edits die with it.
                        self.template_modal.exit_form();
                    } else {
                        self.template_modal.close();
                    }
                }
                // The delete confirmation owns the keys while it is up.
                KeyCode::Enter | KeyCode::Char('d') | KeyCode::Char('D')
                    if self.template_modal.delete_confirm =>
                {
                    self.template_modal.delete_confirm = false;
                    if let Some(template) = self.template_modal.selected_template().cloned()
                        && self.template_manager.delete_template(&template.id).is_ok()
                    {
                        self.refresh_view_list();
                    }
                }
                _ if self.template_modal.delete_confirm => {}
                _ if self.template_modal.score_details.is_some() => {}
                // The list.
                KeyCode::Up | KeyCode::Char('k') if !form => self.template_modal.select_prev(),
                KeyCode::Down | KeyCode::Char('j') if !form => self.template_modal.select_next(),
                KeyCode::Enter if !form => {
                    if let Some(template) = self.template_modal.selected_template().cloned() {
                        if let Err(e) = self.apply_template(&template) {
                            // The list stays open, so the user sees what failed.
                            self.error_modal.show(format!("Error applying view: {}", e));
                        } else {
                            self.template_modal.active = false;
                        }
                    }
                }
                KeyCode::Char('s') if !form => {
                    // A view saved from an untouched table would carry
                    // nothing, and — matching by schema — it would shadow
                    // real views in the V/auto-apply gate as a well-used
                    // no-op. Refuse at the door, not after the form.
                    if self
                        .data_table_state
                        .as_ref()
                        .is_some_and(|state| state.is_at_defaults())
                    {
                        // A refusal is validation, not a failure: it is said on
                        // the surface's own status line, not in a modal.
                        self.template_modal.status = Some(
                            "Nothing to save yet: set a query, filter, sort, column layout, or pivot/melt first."
                                .to_string(),
                        );
                    } else {
                        self.open_save_view_form();
                    }
                }
                KeyCode::Char('e') if !form => {
                    if let Some(template) = self.template_modal.selected_template().cloned() {
                        self.template_modal.enter_edit_mode(
                            &template,
                            self.history_limit,
                            &self.theme,
                        );
                    }
                }
                KeyCode::Char('d') if !form => {
                    if self.template_modal.selected_template().is_some() {
                        self.template_modal.delete_confirm = true;
                    }
                }
                KeyCode::Char('i') if !form => {
                    self.template_modal.score_details = self.view_score_details();
                }
                // The form.
                KeyCode::Tab if form => self.template_modal.next_focus(),
                KeyCode::BackTab if form => self.template_modal.prev_focus(),
                // Ctrl+J too: it works on every terminal, and some send
                // Ctrl+Enter as Ctrl+J.
                KeyCode::Enter | KeyCode::Char('j') if form && ctrl => self.save_view_form(),
                KeyCode::Enter if form => {
                    // Enter saves from anywhere; inside the multiline
                    // description it types, and the footer names Ctrl+J.
                    if self.template_modal.form_focus == FormFocus::Description {
                        let event = KeyEvent::new(KeyCode::Enter, KeyModifiers::empty());
                        self.template_modal
                            .description_input
                            .handle_key(&event, None);
                    } else {
                        self.save_view_form();
                    }
                }
                KeyCode::Up | KeyCode::Down
                    if form && self.template_modal.form_focus == FormFocus::Description =>
                {
                    let event = KeyEvent::new(event.code, KeyModifiers::empty());
                    self.template_modal
                        .description_input
                        .handle_key(&event, None);
                }
                KeyCode::Up if form => self.template_modal.prev_focus(),
                KeyCode::Down if form => self.template_modal.next_focus(),
                KeyCode::PageUp | KeyCode::PageDown
                    if form && self.template_modal.form_focus == FormFocus::Description =>
                {
                    // PageUp/PageDown move through the description five lines at a time.
                    const DESCRIPTION_PAGE_LINES: isize = 5;
                    let delta = if event.code == KeyCode::PageUp {
                        -DESCRIPTION_PAGE_LINES
                    } else {
                        DESCRIPTION_PAGE_LINES
                    };
                    self.template_modal
                        .description_input
                        .move_cursor_by_lines(delta);
                }
                KeyCode::Char(' ')
                    if form && self.template_modal.form_focus == FormFocus::Matching =>
                {
                    self.template_modal.toggle_matching();
                }
                KeyCode::Char(' ')
                    if form && self.template_modal.form_focus == FormFocus::SchemaMatch =>
                {
                    self.template_modal.schema_match_enabled =
                        !self.template_modal.schema_match_enabled;
                }
                KeyCode::Char(_) if form => {
                    if self.template_modal.form_focus == FormFocus::Name {
                        // The error clears as soon as the name changes.
                        self.template_modal.name_error = None;
                    }
                    // The event goes through whole: text fields keep their
                    // readline bindings, so Ctrl+W must arrive as Ctrl+W.
                    if let Some(input) = self.template_modal.focused_input_mut() {
                        input.handle_key(event, None);
                    }
                }
                KeyCode::Backspace
                | KeyCode::Delete
                | KeyCode::Left
                | KeyCode::Right
                | KeyCode::Home
                | KeyCode::End
                    if form =>
                {
                    if self.template_modal.form_focus == FormFocus::Name {
                        self.template_modal.name_error = None;
                    }
                    if let Some(input) = self.template_modal.focused_input_mut() {
                        input.handle_key(event, None);
                    }
                }
                _ => {}
            }
            return None;
        }

        if self.input_mode == InputMode::Editing {
            if self.input_type == Some(InputType::Search) {
                const RIGHT_KEYS: [KeyCode; 2] = [KeyCode::Right, KeyCode::Char('l')];
                const LEFT_KEYS: [KeyCode; 2] = [KeyCode::Left, KeyCode::Char('h')];

                // One chord switches the mode from anywhere in the prompt: in
                // the input ←/→ belong to the cursor, so the tab bar alone
                // cost four keys.
                if event.is_press()
                    && event.modifiers == KeyModifiers::CONTROL
                    && event.code == KeyCode::Char('t')
                {
                    self.set_query_mode(self.query_mode.next());
                    return None;
                }

                if self.query_focus == QueryFocus::TabBar && event.is_press() {
                    // Enter included: it must never dead-end, so from the tab
                    // bar it returns to the input, one keystroke from running.
                    if event.code == KeyCode::BackTab
                        || event.code == KeyCode::Enter
                        || (event.code == KeyCode::Tab
                            && !event.modifiers.contains(KeyModifiers::SHIFT))
                    {
                        self.query_focus = QueryFocus::Input;
                        self.sync_query_focus();
                        return None;
                    }
                    if RIGHT_KEYS.contains(&event.code) {
                        self.set_query_mode(self.query_mode.next());
                        return None;
                    }
                    if LEFT_KEYS.contains(&event.code) {
                        self.set_query_mode(self.query_mode.prev());
                        return None;
                    }
                    if event.code == KeyCode::Esc {
                        self.close_query_prompt();
                    }
                    return None;
                }

                if event.is_press()
                    && event.code == KeyCode::Tab
                    && !event.modifiers.contains(KeyModifiers::SHIFT)
                {
                    self.query_focus = QueryFocus::TabBar;
                    self.sync_query_focus();
                    return None;
                }

                if self.query_focus != QueryFocus::Input {
                    return None;
                }

                self.sync_query_focus();
                let mode = self.query_mode;
                let input = match mode {
                    QueryMode::Sql => &mut self.sql_input,
                    QueryMode::Search => &mut self.fuzzy_input,
                    QueryMode::QStyle => &mut self.query_input,
                };
                match input.handle_key(event, Some(&self.cache)) {
                    TextInputEvent::Submit => {
                        let _ = input.save_to_history(&self.cache);
                        let text = input.value().to_string();
                        return Some(match mode {
                            QueryMode::Sql => AppEvent::SqlSearch(text),
                            QueryMode::Search => AppEvent::FuzzySearch(text),
                            QueryMode::QStyle => AppEvent::Search(text),
                        });
                    }
                    TextInputEvent::Cancel => self.close_query_prompt(),
                    TextInputEvent::HistoryChanged | TextInputEvent::None => {}
                }
                return None;
            }

            // Line number input (GoToLine): ":" then type line number, Enter to jump, Esc to cancel
            if self.input_type == Some(InputType::GoToLine) {
                // The prompt borrows `query_input`, whose history is the query
                // history: without this, ↑ filled the line with a past query
                // and Enter on it closed silently.
                let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
                if matches!(event.code, KeyCode::Up | KeyCode::Down)
                    || (ctrl && matches!(event.code, KeyCode::Char('p' | 'n')))
                {
                    return None;
                }
                self.query_input.set_focused(true);
                let result = self.query_input.handle_key(event, None);
                match result {
                    TextInputEvent::Submit => {
                        let value = self.query_input.value().trim().to_string();
                        self.query_input.clear();
                        self.query_input.set_focused(false);
                        self.input_mode = InputMode::Normal;
                        self.input_type = None;
                        if let Some(state) = &mut self.data_table_state
                            && let Ok(display_line) = value.parse::<usize>()
                        {
                            let row_index = display_line.saturating_sub(state.row_start_index());
                            let would_collect = state.scroll_would_trigger_collect(
                                row_index as i64 - state.start_row as i64,
                            );
                            if would_collect {
                                self.busy = true;
                                return Some(AppEvent::GoToLine(row_index));
                            }
                            state.scroll_to_row_centered(row_index);
                        }
                    }
                    TextInputEvent::Cancel => {
                        self.query_input.clear();
                        self.query_input.set_focused(false);
                        self.input_mode = InputMode::Normal;
                        self.input_type = None;
                    }
                    TextInputEvent::HistoryChanged | TextInputEvent::None => {}
                }
                return None;
            }

            return None;
        }

        const RIGHT_KEYS: [KeyCode; 2] = [KeyCode::Right, KeyCode::Char('l')];

        const LEFT_KEYS: [KeyCode; 2] = [KeyCode::Left, KeyCode::Char('h')];

        const DOWN_KEYS: [KeyCode; 2] = [KeyCode::Down, KeyCode::Char('j')];

        const UP_KEYS: [KeyCode; 2] = [KeyCode::Up, KeyCode::Char('k')];

        // The letter arms below are unmodified keys. Without this guard the
        // bare-`Char` matches also fired with Ctrl or Alt held, so Ctrl+E
        // opened Export and Ctrl+R reversed — bindings nobody declared.
        // Paging (Ctrl+F/B/D/U) is the only modified set this match owns;
        // the global escapes were handled before reaching here.
        if event
            .modifiers
            .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
            && !matches!(event.code, KeyCode::Char('f' | 'b' | 'd' | 'u'))
        {
            return None;
        }

        match event.code {
            // q pops the context: opened from the home screen, it returns
            // there; launched straight onto a file, it quits as it always
            // has. Q and Ctrl+Q stay unconditional.
            KeyCode::Char('q') => {
                if self.opened_from_home {
                    self.enter_home();
                    None
                } else {
                    Some(AppEvent::Exit)
                }
            }
            KeyCode::Char('Q') => Some(AppEvent::Exit),
            KeyCode::Char('R') => Some(AppEvent::Reset),
            // Read the dataset again with its first row the other way: as column names,
            // or as data under `column_1`, `column_2`, …. Only delimited text has a
            // header to turn off; anything else carries its own names, and this does
            // nothing there.
            KeyCode::Char('H') => {
                let (paths, options) = self.opened.clone()?;
                options.format.and_then(FileFormat::separator)?;
                let options = OpenOptions {
                    has_header: Some(!options.has_header.unwrap_or(true)),
                    ..options
                };
                self.set_loading_phase("Scanning input", 10);
                self.name_what_is_loading(paths[0].clone());
                Some(AppEvent::Open(paths, options))
            }
            KeyCode::Char('N') => {
                if let Some(ref mut state) = self.data_table_state {
                    state.toggle_row_numbers();
                }
                None
            }
            KeyCode::Char('D') => {
                // The type row is drawn from the schema the table already has, so
                // this is a render-time flip like `F`. Session-only.
                self.dtype_row = !self.dtype_row;
                if self.debug.enabled {
                    self.debug.last_action = format!(
                        "toggle_dtype_row({})",
                        if self.dtype_row { "on" } else { "off" }
                    );
                }
                None
            }
            KeyCode::Char('F') => {
                // Formatting is applied at render time, so this takes effect on
                // the next frame with no re-collect. Session-only: the config
                // file stays the source of truth at launch.
                self.number_format.enabled = !self.number_format.enabled;
                if self.debug.enabled {
                    self.debug.last_action = format!(
                        "toggle_number_format({})",
                        if self.number_format.enabled {
                            "on"
                        } else {
                            "off"
                        }
                    );
                }
                None
            }
            KeyCode::Esc => {
                // First check if we're in drill-down mode
                let drilled_up = if let Some(ref mut state) = self.data_table_state {
                    if state.is_drilled_down() {
                        state.defer_collect = true;
                        let _ = state.drill_up();
                        state.defer_collect = false;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };
                if drilled_up {
                    self.sync_sort_filter_modal();
                }
                if drilled_up {
                    self.spawn_async_collect(Self::LOADING_BUFFER);
                    return None;
                }
                // Escape no longer exits - use 'q' or Ctrl-C to exit
                // (Info modal handles Esc in its own block)
                None
            }
            code if RIGHT_KEYS.contains(&code) => {
                if let Some(ref mut state) = self.data_table_state {
                    state.scroll_right();
                    if self.debug.enabled {
                        self.debug.last_action = "scroll_right".to_string();
                    }
                }
                None
            }
            code if LEFT_KEYS.contains(&code) => {
                if let Some(ref mut state) = self.data_table_state {
                    state.scroll_left();
                    if self.debug.enabled {
                        self.debug.last_action = "scroll_left".to_string();
                    }
                }
                None
            }
            code if event.is_press() && DOWN_KEYS.contains(&code) => {
                let would_collect = self
                    .data_table_state
                    .as_ref()
                    .map(|s| s.scroll_would_trigger_collect(1))
                    .unwrap_or(false);
                if would_collect {
                    self.busy = true;
                    Some(AppEvent::DoScrollNext)
                } else {
                    if let Some(ref mut s) = self.data_table_state {
                        s.select_next();
                    }
                    None
                }
            }
            code if event.is_press() && UP_KEYS.contains(&code) => {
                let would_collect = self
                    .data_table_state
                    .as_ref()
                    .map(|s| s.scroll_would_trigger_collect(-1))
                    .unwrap_or(false);
                if would_collect {
                    self.busy = true;
                    Some(AppEvent::DoScrollPrev)
                } else {
                    if let Some(ref mut s) = self.data_table_state {
                        s.select_previous();
                    }
                    None
                }
            }
            KeyCode::PageDown if event.is_press() => {
                let would_collect = self
                    .data_table_state
                    .as_ref()
                    .map(|s| s.scroll_would_trigger_collect(s.visible_rows as i64))
                    .unwrap_or(false);
                if would_collect {
                    self.busy = true;
                    Some(AppEvent::DoScrollDown)
                } else {
                    if let Some(ref mut s) = self.data_table_state {
                        s.page_down();
                    }
                    None
                }
            }
            KeyCode::Home if event.is_press() => self.jump_key(AppEvent::DoScrollHome),
            KeyCode::End | KeyCode::Char('G') if event.is_press() => {
                self.jump_key(AppEvent::DoScrollEnd)
            }
            KeyCode::Char('f')
                if event.modifiers.contains(KeyModifiers::CONTROL) && event.is_press() =>
            {
                let would_collect = self
                    .data_table_state
                    .as_ref()
                    .map(|s| s.scroll_would_trigger_collect(s.visible_rows as i64))
                    .unwrap_or(false);
                if would_collect {
                    self.busy = true;
                    Some(AppEvent::DoScrollDown)
                } else {
                    if let Some(ref mut s) = self.data_table_state {
                        s.page_down();
                    }
                    None
                }
            }
            KeyCode::Char('b')
                if event.modifiers.contains(KeyModifiers::CONTROL) && event.is_press() =>
            {
                let would_collect = self
                    .data_table_state
                    .as_ref()
                    .map(|s| s.scroll_would_trigger_collect(-(s.visible_rows as i64)))
                    .unwrap_or(false);
                if would_collect {
                    self.busy = true;
                    Some(AppEvent::DoScrollUp)
                } else {
                    if let Some(ref mut s) = self.data_table_state {
                        s.page_up();
                    }
                    None
                }
            }
            KeyCode::Char('d')
                if event.modifiers.contains(KeyModifiers::CONTROL) && event.is_press() =>
            {
                let half = self
                    .data_table_state
                    .as_ref()
                    .map(|s| (s.visible_rows / 2).max(1) as i64)
                    .unwrap_or(1);
                let would_collect = self
                    .data_table_state
                    .as_ref()
                    .map(|s| s.scroll_would_trigger_collect(half))
                    .unwrap_or(false);
                if would_collect {
                    self.busy = true;
                    Some(AppEvent::DoScrollHalfDown)
                } else {
                    if let Some(ref mut s) = self.data_table_state {
                        s.half_page_down();
                    }
                    None
                }
            }
            KeyCode::Char('u')
                if event.modifiers.contains(KeyModifiers::CONTROL) && event.is_press() =>
            {
                let half = self
                    .data_table_state
                    .as_ref()
                    .map(|s| (s.visible_rows / 2).max(1) as i64)
                    .unwrap_or(1);
                let would_collect = self
                    .data_table_state
                    .as_ref()
                    .map(|s| s.scroll_would_trigger_collect(-half))
                    .unwrap_or(false);
                if would_collect {
                    self.busy = true;
                    Some(AppEvent::DoScrollHalfUp)
                } else {
                    if let Some(ref mut s) = self.data_table_state {
                        s.half_page_up();
                    }
                    None
                }
            }
            KeyCode::PageUp if event.is_press() => {
                let would_collect = self
                    .data_table_state
                    .as_ref()
                    .map(|s| s.scroll_would_trigger_collect(-(s.visible_rows as i64)))
                    .unwrap_or(false);
                if would_collect {
                    self.busy = true;
                    Some(AppEvent::DoScrollUp)
                } else {
                    if let Some(ref mut s) = self.data_table_state {
                        s.page_up();
                    }
                    None
                }
            }
            KeyCode::Enter if event.is_press() => {
                if self.input_mode != InputMode::Normal {
                    return None;
                }
                let state = self.data_table_state.as_ref()?;
                let selected = state.table_state.selected()?;
                let group_index = state.start_row + selected;
                match state.drill_row(group_index) {
                    None if state.is_drilled_down() => {
                        self.flash_note("Already in a group; Esc goes back".to_string());
                    }
                    None => self.flash_note("Nothing to drill into".to_string()),
                    Some(DrillRow::Buffered(row)) => self.drill_into(group_index, &row),
                    Some(DrillRow::Read(lf)) => {
                        let streaming = state.polars_streaming;
                        self.spawn_bg(Self::READING_GROUP, move |task_gen, tx| {
                            let row = crate::statistics::collect_lazy(*lf, streaming)
                                .map_err(|e| crate::error_display::user_message_from_polars(&e));
                            let _ = tx.send(AppEvent::DrillRowRead {
                                generation: task_gen,
                                group_index,
                                row,
                            });
                        });
                    }
                }
                None
            }
            KeyCode::Char('i') if event.is_press() => {
                if let Some(state) = self.data_table_state.as_mut() {
                    // Unread notes put the panel's Notes tab in front — that is
                    // what the accented `i` chip was promising. Read before the
                    // mark, which is what retires the accent.
                    let unseen = state.notes_unseen();
                    state.mark_notes_seen();
                    if unseen {
                        self.info_modal
                            .open_on(crate::widgets::info::InfoTab::Notes);
                    } else {
                        self.info_modal.open();
                    }
                    self.input_mode = InputMode::Info;
                    // Defer Parquet metadata load so UI can show throbber; avoid blocking in render
                    if self.path.is_some()
                        && self.original_file_format == Some(ExportFormat::Parquet)
                        && self.parquet_metadata_cache.is_none()
                    {
                        self.busy = true;
                        return Some(AppEvent::DoLoadParquetMetadata);
                    }
                }
                None
            }
            KeyCode::Char('/') => {
                self.input_mode = InputMode::Editing;
                self.input_type = Some(InputType::Search);
                self.query_mode = self.opening_query_mode();
                self.query_focus = QueryFocus::Input;
                if let Some(state) = &mut self.data_table_state {
                    self.query_input.set_value(state.active_query.clone());
                    self.sql_input.set_value(state.get_active_sql_query());
                    self.fuzzy_input.set_value(state.get_active_fuzzy_query());
                    // The restored query arrives selected: typing states a new
                    // question, arrows edit the old one. Unselected, typing
                    // appended to the tail of the last query.
                    self.query_input.select_all();
                    self.sql_input.select_all();
                    self.fuzzy_input.select_all();
                    state.suppress_error_display = true;
                } else {
                    self.query_input.clear();
                    self.sql_input.clear();
                    self.fuzzy_input.clear();
                }
                self.sync_query_focus();
                None
            }
            KeyCode::Char(':') if event.is_press() => {
                if self.data_table_state.is_some() {
                    self.input_mode = InputMode::Editing;
                    self.input_type = Some(InputType::GoToLine);
                    self.query_input.clear();
                    self.query_input.set_focused(true);
                }
                None
            }
            KeyCode::Char('V') => {
                // Apply the best view whose criteria match this dataset. When none
                // does, the answer is not silence and not the best-scored stranger: the
                // list opens, so the user sees what exists and picks — or saves one.
                if let Some(ref state) = self.data_table_state
                    && let Some(ref path) = self.path
                {
                    match self
                        .template_manager
                        .get_most_relevant(path, state.source_schema())
                    {
                        Some(template) => {
                            if let Err(e) = self.apply_template(&template) {
                                self.error_modal.show(format!("Error applying view: {}", e));
                            }
                        }
                        None => self.open_template_list(),
                    }
                }
                None
            }
            KeyCode::Char('v') => {
                if self.data_table_state.is_some() && self.path.is_some() {
                    self.open_template_list();
                }
                None
            }
            KeyCode::Char('s') => {
                if self.data_table_state.is_some() {
                    // Rebuilt from the table's applied state, never from what the modal
                    // held last time: an edit staged and then canceled must not arrive
                    // pre-staged, one Apply away from committing silently.
                    self.sync_sort_filter_modal();
                    self.sort_filter_modal.open(self.history_limit, &self.theme);
                    self.input_mode = InputMode::SortFilter;
                }
                None
            }
            KeyCode::Char('r') => {
                if let Some(state) = &mut self.data_table_state {
                    state.reverse();
                }
                None
            }
            KeyCode::Char('a') => {
                // Open analysis modal; no computation until user selects a tool from the sidebar (Enter)
                if self.data_table_state.is_some()
                    && self.input_mode == InputMode::Normal
                    && self.quality_evidence_return.is_none()
                {
                    self.analysis_modal.open();
                    // The sample outlives a close, but its scope names this
                    // dataset's rows: another dataset starts from its current view.
                    if self.analysis_modal.sample_dataset != Some(self.dataset_generation) {
                        self.analysis_modal.sample.scope = data_quality::QualityScope::CurrentView;
                        self.analysis_modal.sample_dataset = Some(self.dataset_generation);
                    }
                }
                None
            }
            KeyCode::Char('c') => {
                if let Some(state) = &self.data_table_state
                    && self.input_mode == InputMode::Normal
                {
                    let numeric_columns: Vec<String> = state
                        .schema
                        .iter()
                        .filter(|(_, dtype)| dtype.is_numeric())
                        .map(|(name, _)| name.to_string())
                        .collect();
                    let datetime_columns: Vec<String> = state
                        .schema
                        .iter()
                        .filter(|(_, dtype)| {
                            matches!(
                                dtype,
                                DataType::Datetime(_, _) | DataType::Date | DataType::Time
                            )
                        })
                        .map(|(name, _)| name.to_string())
                        .collect();
                    self.chart_modal.open(
                        &numeric_columns,
                        &datetime_columns,
                        self.app_config.chart.row_limit,
                    );
                    self.chart_cache.clear();
                    self.input_mode = InputMode::Chart;
                }
                None
            }
            KeyCode::Char('p') => {
                if let Some(state) = &self.data_table_state
                    && self.input_mode == InputMode::Normal
                {
                    self.pivot_melt_modal.available_columns =
                        state.schema.iter_names().map(|s| s.to_string()).collect();
                    self.pivot_melt_modal.column_dtypes = state
                        .schema
                        .iter()
                        .map(|(n, d)| (n.to_string(), d.clone()))
                        .collect();
                    self.pivot_melt_modal.open(self.history_limit, &self.theme);
                    self.input_mode = InputMode::PivotMelt;
                }
                None
            }
            KeyCode::Char('e') => {
                if self.data_table_state.is_some() && self.input_mode == InputMode::Normal {
                    self.export_modal.open(
                        self.original_file_format,
                        self.history_limit,
                        &self.theme,
                        self.original_file_delimiter,
                    );
                    self.export_modal.offer_source_file = self
                        .data_table_state
                        .as_ref()
                        .is_some_and(|state| state.can_name_source_files());
                    self.input_mode = InputMode::Export;
                }
                None
            }
            KeyCode::Char('y') => {
                if self.input_mode == InputMode::Normal
                    && let Some(state) = self.data_table_state.as_ref()
                {
                    let columns = state.get_column_order().to_vec();
                    let context = copy_modal::CopyContext {
                        row_number: state.selected_display_row().unwrap_or(0),
                        view_rows: state.copy_view_df().map(|d| d.height()).unwrap_or(0),
                        view_cols: columns.len(),
                        total_rows: state.num_rows_if_valid(),
                    };
                    self.copy_modal.open(columns, context);
                    self.input_mode = InputMode::Copy;
                }
                None
            }
            _ => None,
        }
    }

    /// Handle one event. A key that arrives while the app is busy is not acted on and
    /// not dropped either: it comes back as `Err(key)` for the caller to hold until the
    /// app is idle. The main loop ([`event_pump::EventPump`]) does exactly that;
    /// [`App::event`] is the same call for callers that have nowhere to hold a key.
    pub fn handle(&mut self, event: &AppEvent) -> EventOutcome {
        if let AppEvent::Key(key) = event
            && self.busy
            && !self.key_acts_while_busy(key)
        {
            return Err(*key);
        }
        let out = self.dispatch_event(event);
        // Not while this handler is returning a continuation. A follow-up is the rest of
        // the event just handled — the analysis sets `computing` and returns
        // `AnalysisChunk`, and the phase that chunk will spawn has not spawned — so
        // nothing holds a lease on the generation yet, and the errands below would bump
        // it out from under the errand that is halfway through. They run after every
        // event and are built to wait; one more event is nothing to them.
        if out.is_none() {
            // Columns a dataset's footers found while the user was inside a query are
            // held rather than dropped; this is where they get in, on the first event
            // after the view comes back to the data.
            if self.join_held_footers() {
                self.reread_after_the_footers_joined();
            }
            // And the same turn for a re-read owed to a dataset whose footers could not
            // be read: it waits on the same work, and gets in the same way.
            self.reread_when_the_work_allows();
            self.collect_when_the_work_allows();
        }
        self.ensure_chart_data();
        Ok(out)
    }

    pub fn event(&mut self, event: &AppEvent) -> Option<AppEvent> {
        self.handle(event).unwrap_or(None)
    }

    /// True while chart data for the current view is being prepared off-thread — either
    /// its worker is running, or it is waiting its turn behind an orphaned worker that
    /// cannot be cancelled (see `ChartInflight::stale`). Either way the user is waiting
    /// on a computation and the throbber should say so.
    pub fn chart_preparing(&self) -> bool {
        match self.chart_inflight.as_ref() {
            Some(inflight) if !inflight.stale => true,
            Some(_) => self.chart_request_pending(),
            None => false,
        }
    }

    /// Whether the chart view wants data it does not have and cannot be told it will
    /// never get.
    fn chart_request_pending(&self) -> bool {
        if self.input_mode != InputMode::Chart || !self.chart_modal.active {
            return false;
        }
        ChartRequest::from_modal(&self.chart_modal)
            .is_some_and(|request| self.chart_cache.get(&request).is_none())
    }

    /// Forget everything chart-related that belongs to the view or dataset on its way
    /// out: the cache, the handed-over slot, an export parked on data that is now never
    /// coming, and an export write still running (its file may still appear, but its
    /// result is ignored and `busy` is released). The preparation in flight is marked
    /// stale rather than forgotten: it cannot be cancelled, so it is waited for and its
    /// result discarded on arrival. Called when the chart view closes and whenever the
    /// dataset changes or is left for the home screen.
    fn reset_chart_state(&mut self) {
        self.chart_cache.clear();
        if let Some(inflight) = self.chart_inflight.as_mut() {
            inflight.stale = true;
        }
        // A failed export reopens its modal; it must not follow the user to the next
        // dataset.
        self.chart_export_modal.close();
        *self
            .pending_chart_result
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = None;
        let writing = self.chart_export_inflight.take().is_some();
        let waiting = self.chart_export_waiting.take().is_some();
        if writing || waiting {
            self.loading_state = LoadingState::Idle;
            self.status_message = None;
            self.busy = false;
        }
    }

    /// True when the chart cache holds the data for the modal's current selection.
    pub fn chart_data_ready(&self) -> bool {
        ChartRequest::from_modal(&self.chart_modal).is_some_and(|r| self.chart_cache.satisfies(&r))
    }

    /// Start preparing the chart the modal currently asks for, unless the cache already
    /// has it, it is known to fail, or another preparation is still running (the newest
    /// selection is picked up when that one lands). Runs after every event, so a change
    /// of column or option is noticed as soon as it is made and render only ever draws.
    fn ensure_chart_data(&mut self) {
        if self.input_mode != InputMode::Chart || !self.chart_modal.active {
            return;
        }
        let Some(request) = ChartRequest::from_modal(&self.chart_modal) else {
            return;
        };
        if self.chart_cache.get(&request).is_some() {
            self.chart_cache.touch(&request, self.chart_modal.log_scale);
            return;
        }
        if self.chart_inflight.is_some() {
            return;
        }
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let lf = state.lf.clone();
        let schema = state.schema.clone();
        let dataset = Some(state.len_generation());
        let rows = self.chart_modal.effective_row_limit();
        self.chart_inflight = Some(ChartInflight {
            dataset,
            request: request.clone(),
            stale: false,
        });
        let slot = self.pending_chart_result.clone();
        let tx = self.events.clone();
        self.runtime.spawn_blocking(move || {
            // A panic in the preparation must still report back: without the event the
            // in-flight record would stand for the rest of the session and every later
            // selection would be refused.
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                request.prepare(&lf, &schema, rows)
            }))
            .unwrap_or_else(|_| Err(color_eyre::eyre::eyre!("Chart preparation panicked")))
            .map_err(|e| crate::error_display::user_message_from_report(&e, None));
            *slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(result);
            let _ = tx.send(AppEvent::BackgroundChartReady);
        });
    }

    fn dispatch_event(&mut self, event: &AppEvent) -> Option<AppEvent> {
        self.debug.num_events += 1;

        match event {
            AppEvent::Key(key) => self.key(key),
            AppEvent::Open(paths, options) => {
                if paths.is_empty() {
                    return Some(AppEvent::Crash("No paths provided".to_string()));
                }
                // Home is now in the stack, so q pops back to it. Never unset:
                // a reread from the table (H) is not a new place.
                if self.input_mode == InputMode::Home {
                    self.opened_from_home = true;
                }
                // `az://container/path` and its kin name no account; where they were
                // typed, or the config, does.
                #[cfg(feature = "cloud")]
                let expanded = match paths
                    .iter()
                    .map(|p| {
                        crate::cloud_sources::expand_azure_short_url(
                            p,
                            &self.app_config.cloud,
                            self.home.browsing.as_deref(),
                        )
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()
                {
                    Ok(expanded) => expanded,
                    Err(message) => return Some(AppEvent::Crash(message)),
                };
                #[cfg(feature = "cloud")]
                if &expanded != paths {
                    return Some(AppEvent::Open(expanded, options.clone()));
                }
                // The last download goes, unless this is opening it again.
                #[cfg(any(feature = "http", feature = "cloud"))]
                if let Some(p) = self.http_temp_path.take() {
                    if paths.len() == 1 && self.downloaded_from.as_ref() == Some(&paths[0]) {
                        self.http_temp_path = Some(p);
                    } else {
                        let _ = std::fs::remove_file(&p);
                        self.downloaded_from = None;
                    }
                }
                self.reset_chart_state();
                self.task_generation = self.task_generation.wrapping_add(1);
                // A new counter for a new load, and the old counter cancelled: what
                // the last dataset was still reading is no longer wanted, and unread,
                // unpaid-for is better than read and dropped.
                //
                // The meter needs no equivalent: it belongs to the dataset rather than
                // to the app, so a load that never reaches the screen never has one
                // installed. See `DataTableState::measurements`.
                self.footer_progress.cancel();
                self.footer_progress = Arc::new(crate::schema_union::FooterProgress::default());
                // Whatever the last dataset was still reading is no longer wanted.
                *self
                    .pending_footers_result
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()) = None;
                self.load_active = true;
                self.awaiting_dataset = true;
                self.busy = true;
                let first = &paths[0];
                // Every open records a recent, not just those started from the home
                // screen — most datasets are named on the command line, and those are
                // exactly the ones worth getting back to. An object-store URL counts
                // doubly: `s3://bucket/warehouse/events/year=2024` is far more painful
                // to retype than any local path, and it is recorded verbatim, since
                // canonicalising a URL is meaningless.
                //
                // Recorded once the dataset is installed, not here: a file that fails
                // to load is not one anybody wants to get back to. Kept as named, since
                // what is installed may be a download's temporary copy.
                let is_local = matches!(source::input_source(first), source::InputSource::Local(_));
                self.recent_on_install = (!is_local || first.exists()).then(|| first.clone());
                self.opening = Some(paths.clone());
                let file_size = match source::input_source(first) {
                    source::InputSource::Local(_) => {
                        std::fs::metadata(first).map(|m| m.len()).unwrap_or(0)
                    }
                    source::InputSource::S3(_)
                    | source::InputSource::Gcs(_)
                    | source::InputSource::Azure(_)
                    | source::InputSource::Http(_) => 0,
                };
                let path_str = first.as_os_str().to_string_lossy();
                let _is_partitioned_path = paths.len() == 1
                    && options.hive
                    && (first.is_dir() || path_str.contains('*') || path_str.contains("**"));
                let phase = "Scanning input";

                self.loading_state = LoadingState::Loading {
                    file_path: Some(first.clone()),
                    file_size,
                    current_phase: phase.to_string(),
                    progress_percent: 10,
                };

                Some(AppEvent::DoLoadScanPaths(paths.clone(), options.clone()))
            }
            AppEvent::OpenLazyFrame(lf, options) => {
                // A frame handed over has no path to go back to.
                self.recent_on_install = None;
                self.opening = None;
                self.opened = None;
                self.reset_chart_state();
                self.task_generation = self.task_generation.wrapping_add(1);
                // A new counter for a new load, and the old counter cancelled: what
                // the last dataset was still reading is no longer wanted, and unread,
                // unpaid-for is better than read and dropped.
                //
                // The meter needs no equivalent: it belongs to the dataset rather than
                // to the app, so a load that never reaches the screen never has one
                // installed. See `DataTableState::measurements`.
                self.footer_progress.cancel();
                self.footer_progress = Arc::new(crate::schema_union::FooterProgress::default());
                // Whatever the last dataset was still reading is no longer wanted.
                *self
                    .pending_footers_result
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()) = None;
                self.load_active = true;
                self.awaiting_dataset = true;
                self.busy = true;
                self.loading_state = LoadingState::Loading {
                    file_path: None,
                    file_size: 0,
                    current_phase: "Scanning input".to_string(),
                    progress_percent: 10,
                };
                Some(AppEvent::DoLoadSchema(lf.clone(), None, options.clone()))
            }
            AppEvent::DoLoadScanPaths(paths, options) => {
                // The user went home while this load was in flight. The chain stops
                // here; whatever is already running finishes and is discarded.
                if !self.load_active {
                    return None;
                }
                let first = &paths[0];
                let src = source::input_source(first);
                if paths.len() > 1 {
                    match &src {
                        source::InputSource::S3(_) => {
                            return Some(AppEvent::Crash(
                                "Only one S3 URL at a time. Open a single s3:// path.".to_string(),
                            ));
                        }
                        source::InputSource::Gcs(_) => {
                            return Some(AppEvent::Crash(
                                "Only one GCS URL at a time. Open a single gs:// path.".to_string(),
                            ));
                        }
                        source::InputSource::Azure(_) => {
                            return Some(AppEvent::Crash(
                                "Only one Azure URL at a time. Open a single abfss:// path."
                                    .to_string(),
                            ));
                        }
                        source::InputSource::Http(_) => {
                            return Some(AppEvent::Crash(
                                "Only one HTTP/HTTPS URL at a time. Open a single URL.".to_string(),
                            ));
                        }
                        source::InputSource::Local(_) => {}
                    }
                }
                let compression = options
                    .compression
                    .or_else(|| CompressionFormat::from_extension(first));
                let is_csv = options.format == Some(FileFormat::Csv)
                    || first
                        .file_stem()
                        .and_then(|stem| stem.to_str())
                        .map(|stem| {
                            stem.ends_with(".csv")
                                || first
                                    .extension()
                                    .and_then(|e| e.to_str())
                                    .map(|e| e.eq_ignore_ascii_case("csv"))
                                    .unwrap_or(false)
                        })
                        .unwrap_or(false);
                let is_compressed_csv = matches!(src, source::InputSource::Local(_))
                    && paths.len() == 1
                    && compression.is_some()
                    && is_csv;
                if is_compressed_csv {
                    if let LoadingState::Loading {
                        file_path,
                        file_size,
                        ..
                    } = &self.loading_state
                    {
                        self.loading_state = LoadingState::Loading {
                            file_path: file_path.clone(),
                            file_size: *file_size,
                            current_phase: "Decompressing".to_string(),
                            progress_percent: 30,
                        };
                    }
                    Some(AppEvent::DoDecompress(paths.clone(), options.clone()))
                } else {
                    // Opened again, and downloaded already: read the copy on hand. `H`
                    // re-reads a downloaded file this way rather than fetching it again.
                    #[cfg(any(feature = "http", feature = "cloud"))]
                    if paths.len() == 1
                        && self.downloaded_from.as_ref() == Some(&paths[0])
                        && let Some(temp) = self.http_temp_path.clone().filter(|t| t.exists())
                    {
                        return Some(AppEvent::DoLoadFromHttpTemp(temp, options.clone()));
                    }
                    // The size probe is a network round trip, so it runs off the event
                    // thread and the confirmation modal is raised when it answers.
                    #[cfg(feature = "http")]
                    if let source::InputSource::Http(ref url) = src {
                        return self.spawn_remote_size_probe(PendingDownload::Http {
                            url: url.clone(),
                            size: None,
                            options: options.clone(),
                        });
                    }
                    #[cfg(feature = "cloud")]
                    if let source::InputSource::S3(ref url) = src {
                        let full = format!("s3://{url}");
                        let (_, ext) = source::url_path_extension(&full);
                        let is_glob = source::is_prefix_or_glob(&full);
                        if source::cloud_path_should_download(ext.as_deref(), is_glob) {
                            return self.spawn_remote_size_probe(PendingDownload::S3 {
                                url: full,
                                size: None,
                                options: options.clone(),
                            });
                        }
                    }
                    #[cfg(feature = "cloud")]
                    if let source::InputSource::Azure(ref url) = src {
                        let (_, ext) = source::url_path_extension(url);
                        let is_glob = source::is_prefix_or_glob(url);
                        if source::cloud_path_should_download(ext.as_deref(), is_glob) {
                            return self.spawn_remote_size_probe(PendingDownload::Azure {
                                url: url.clone(),
                                size: None,
                                options: options.clone(),
                            });
                        }
                    }
                    #[cfg(feature = "cloud")]
                    if let source::InputSource::Gcs(ref url) = src {
                        let full = format!("gs://{url}");
                        let (_, ext) = source::url_path_extension(&full);
                        let is_glob = source::is_prefix_or_glob(&full);
                        if source::cloud_path_should_download(ext.as_deref(), is_glob) {
                            return self.spawn_remote_size_probe(PendingDownload::Gcs {
                                url: full,
                                size: None,
                                options: options.clone(),
                            });
                        }
                    }
                    // When CSV with --parse-strings, set "Scanning string columns" and defer build so UI can show it before blocking.
                    if paths.len() == 1 && is_csv && options.parse_strings.is_some() {
                        if let LoadingState::Loading {
                            file_path,
                            file_size,
                            ..
                        } = &self.loading_state
                        {
                            self.loading_state = LoadingState::Loading {
                                file_path: file_path.clone(),
                                file_size: *file_size,
                                current_phase: "Scanning string columns".to_string(),
                                progress_percent: 55,
                            };
                        }
                        return Some(AppEvent::DoLoadCsvWithParseStrings(
                            paths.clone(),
                            options.clone(),
                        ));
                    }
                    #[allow(clippy::needless_borrow)]
                    self.spawn_scan("Scanning input...", paths.clone(), options.clone())
                }
            }
            AppEvent::HomeListingReady {
                generation,
                listing,
            } => {
                // Clear the flag first, whatever the generation: a stale result that
                // returned early while still marked in flight would wedge the pipeline
                // permanently, and nothing would ever be listed again.
                self.home.listing_in_flight = false;
                // A listing from a superseded request describes somewhere the user has
                // already left.
                if *generation != self.home_generation {
                    return None;
                }
                self.home.apply_listing((**listing).clone());
                // Probes are chosen from the sections, so they can only be started
                // once those exist — asking before the listing lands finds nothing.
                self.spawn_home_probes();
                #[cfg(feature = "cloud")]
                self.spawn_cloud_discovery();
                self.request_home_measurements();
                self.request_home_classifications();
                None
            }
            AppEvent::HomeMeasured { measured, done } => {
                for (path, m) in measured {
                    self.home.enriched.insert(path.clone(), m.clone());
                }
                self.home.apply_measurements();
                if *done {
                    self.home.measure_in_flight = false;
                    self.request_home_measurements();
                }
                None
            }
            AppEvent::HomeClassified { measured, done } => {
                // Kept even when the listing has been rebuilt since it was asked for. A
                // probe or a cloud peek landing rebuilds it, and a Recent section full of
                // buckets lands several in a row: dropping the answer each time left a
                // share's rows unlabeled for as long as the cloud kept answering.
                for (path, m) in measured {
                    self.home.enriched.insert(path.clone(), m.clone());
                }
                // Nothing re-sorts. `apply_measurements` writes the kind into the row
                // where it already is, which is the whole reason a kind is allowed to
                // arrive after the row was drawn: a listing that reshuffled itself
                // under the cursor while it filled in would be worse than a late
                // label.
                self.home.apply_measurements();
                // The next batch is chosen from the viewport as it is now, so a page
                // that scrolled past four hundred rows while this one was out asks
                // about the forty it landed on, not the four hundred it left behind.
                if *done {
                    self.home.classify_in_flight = false;
                    self.request_home_classifications();
                }
                None
            }
            AppEvent::HomePathCompleted {
                generation,
                typed,
                completed,
                candidates,
            } => {
                // Discard if the user has typed since asking: completing onto a
                // different string would scramble what they are in the middle of.
                if *generation != self.home_generation || &self.home.path_input != typed {
                    return None;
                }
                if *candidates == 0 {
                    self.home.status = Some("No such path".to_string());
                } else {
                    self.home.status = None;
                    if *candidates > 1 {
                        self.flash_note(format!("{candidates} matches"));
                    }
                    self.home.path_input = completed.clone();
                }
                None
            }
            AppEvent::HomeSchemaReady {
                generation,
                path,
                preview,
            } => {
                self.home_schema_inflight.retain(|p| p != path);
                if *generation == self.home_generation {
                    self.home_schema_cache.insert(path.clone(), preview.clone());
                }
                None
            }
            AppEvent::HomeSearchBatch {
                generation,
                root,
                found,
                scanned,
            } => {
                // Results from a walk that a later navigation superseded describe a
                // place the user has left. The walk is abandoned, not cancelled, so
                // late batches are expected rather than exceptional.
                if *generation == self.home_generation {
                    self.home.search_batch(root, found.clone(), *scanned);
                }
                None
            }
            AppEvent::HomeSearchDone {
                generation,
                root,
                scanned,
                limited,
            } => {
                if *generation == self.home_generation {
                    self.home.search_finished(root, *scanned, limited.clone());
                }
                self.home_search_inflight = false;
                None
            }
            #[cfg(feature = "cloud")]
            AppEvent::HomeCloudSources { sources } => {
                self.home.cloud = sources.clone();
                self.home_refresh();
                None
            }
            #[cfg(feature = "cloud")]
            AppEvent::HomeCloudListed {
                id,
                buckets,
                details,
                failure,
                listed_at,
            } => {
                if let Some(source) = self.home.cloud.iter_mut().find(|s| &s.id == id) {
                    source.refreshing = false;
                    for (place, lines) in details {
                        source.place_details.insert(place.clone(), lines.clone());
                    }
                    match failure {
                        // A refresh that failed keeps what the last one found: stale
                        // buckets are more use than none, and the row says it failed.
                        Some((short, detail)) => {
                            for bucket in buckets {
                                if !source.buckets.contains(bucket) {
                                    source.buckets.push(bucket.clone());
                                }
                            }
                            source.status = home::CloudStatus::Failed {
                                short: short.clone(),
                                detail: detail.clone(),
                            };
                        }
                        None => {
                            source.buckets = buckets.clone();
                            source.status = home::CloudStatus::Listed;
                            source.listed_at = Some(*listed_at);
                        }
                    }
                }
                self.home_refresh();
                None
            }
            AppEvent::HomeProbeFailed { root, message } => {
                self.home_probes_inflight.retain(|p| p != root);
                self.home.probe_failed(root.clone());
                self.home.probe_errors.insert(root.clone(), message.clone());
                self.home_refresh();
                None
            }
            AppEvent::HomeProbeProgress { root, rows } => {
                // Only while that listing is still out: a late batch must not paint
                // over the whole answer.
                if self.home_probes_inflight.contains(root) && !self.home.probed.contains_key(root)
                {
                    self.home.listing_so_far.insert(root.clone(), rows.clone());
                    self.home_refresh();
                }
                None
            }
            AppEvent::HomeProbeReady {
                root,
                rows,
                cut_short,
            } => {
                // Give the slot back. The cap exists to bound threads wedged on a dead
                // `hard` mount, which never send this event and so keep their slot for
                // good — a probe that answered is not one of those. Without this the
                // list only grows, and after MAX_CONCURRENT_PROBES roots no further
                // root is ever probed for the rest of the session.
                self.home_probes_inflight.retain(|p| p != root);
                let landed = rows.is_some();
                match rows {
                    Some(rows) => self.home.probe_ready(root.clone(), rows.clone()),
                    None => self.home.probe_failed(root.clone()),
                }
                if *cut_short {
                    self.home.cut_short.insert(root.clone());
                }
                // An account read with its keys because the sign-in has no data role
                // says so beside the account.
                #[cfg(feature = "cloud")]
                if let Some((account, _, _)) = source::azure_parts(&root.to_string_lossy())
                    && crate::azure::remembered_key(&account).is_some()
                {
                    for source in &mut self.home.cloud {
                        let place = source
                            .buckets
                            .iter()
                            .find(|b| home::cloud_account(b).is_some_and(|(_, a)| a == account))
                            .cloned();
                        if let Some(place) = place {
                            let lines = source.place_details.entry(place).or_default();
                            if !lines.iter().any(|(k, _)| k == "access") {
                                lines.push(("access".to_string(), "access key".to_string()));
                            }
                        }
                    }
                }
                // Rebuild so the listing picks the result up; the probe is the only
                // thing that ever reads a remote root.
                self.home_refresh();
                // And then ask about the rows it brought. After the rebuild, never
                // before: the picker reads `visible()`, which is written by the
                // rebuild, so a peek asked between `probe_ready` and here looks at the
                // previous listing and finds nothing in it to ask about.
                //
                // Asked here at all because a listing that lands while the cursor is
                // already where it will stay may draw no further frame, and the frame
                // is what otherwise notices.
                #[cfg(feature = "cloud")]
                if landed {
                    self.peek_cloud_directories();
                }
                #[cfg(not(feature = "cloud"))]
                let _ = landed;
                None
            }
            AppEvent::HomeCloudKinds { kinds, failed } => {
                for directory in failed {
                    self.home.peeking.remove(directory);
                    self.home.peek_failed.insert(directory.clone());
                }
                let roots: Vec<PathBuf> = self.home.probed.keys().cloned().collect();
                for (directory, kind) in kinds {
                    // Answered: out of the in-flight set and into the one the rows are
                    // labelled from. Every directory asked about comes back, so nothing
                    // stays in `peeking` and nothing is asked twice.
                    self.home.peeking.remove(directory);
                    self.home
                        .cloud_kinds
                        .insert(directory.clone(), kind.clone());
                }
                for root in roots {
                    self.home.apply_cloud_kinds(&root);
                }
                self.home_refresh();
                None
            }
            AppEvent::BackgroundLazyFrameReady {
                generation,
                path,
                options,
            } => {
                // A scan that a newer open has already superseded is dropped on the
                // floor: its LazyFrame describes data nobody is looking at any more.
                // The slot is deliberately left alone here — a newer task may already
                // have written its result into it, and taking would discard that.
                if *generation != self.task_generation || !self.load_active {
                    return None;
                }
                let (slot_gen, lf) = self
                    .pending_lazyframe_result
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take()?;
                if slot_gen != self.task_generation {
                    return None;
                }

                if let LoadingState::Loading {
                    file_path,
                    file_size,
                    ..
                } = &self.loading_state
                {
                    self.loading_state = LoadingState::Loading {
                        file_path: file_path.clone(),
                        file_size: *file_size,
                        current_phase: "Caching schema".to_string(),
                        progress_percent: 40,
                    };
                }
                Some(AppEvent::DoLoadSchema(
                    Box::new(lf),
                    path.clone(),
                    options.clone(),
                ))
            }
            AppEvent::DoLoadCsvWithParseStrings(paths, options) => {
                if !self.load_active {
                    return None;
                }
                self.spawn_scan("Scanning string columns...", paths.clone(), options.clone())
            }
            #[cfg(feature = "http")]
            AppEvent::DoDownloadHttp(url, options) => {
                if !self.load_active {
                    return None;
                }
                let url = url.clone();
                let options = options.clone();
                self.spawn_bg("Downloading...", move |task_gen, tx| {
                    let ext = source::download_suffix(url.as_str());
                    match Self::download_http_to_temp(
                        url.as_str(),
                        options.temp_dir.as_deref(),
                        ext.as_deref(),
                    ) {
                        Ok(temp_path) => {
                            let _ = tx.send(AppEvent::BackgroundDownloadReady {
                                generation: task_gen,
                                temp_path,
                                options,
                            });
                        }
                        Err(e) => {
                            let _ = tx.send(AppEvent::BackgroundError {
                                generation: task_gen,
                                message: crate::error_display::user_message_from_report(&e, None),
                            });
                        }
                    }
                });
                None
            }
            #[cfg(feature = "cloud")]
            AppEvent::DoDownloadS3ToTemp(url, options)
            | AppEvent::DoDownloadGcsToTemp(url, options) => {
                if !self.load_active {
                    return None;
                }
                let url = url.clone();
                let options = options.clone();
                let cloud_config = self.app_config.cloud.clone();
                let rt = self.runtime.clone();
                let status = match source::input_source(Path::new(&url)) {
                    source::InputSource::Gcs(_) => "Downloading from GCS...",
                    source::InputSource::Azure(_) => "Downloading from Azure...",
                    _ => "Downloading from S3...",
                };
                self.spawn_bg(
                    status,
                    move |task_gen, tx| match Self::download_cloud_to_temp(
                        &url,
                        &cloud_config,
                        &options,
                        &rt,
                    ) {
                        Ok(temp_path) => {
                            let _ = tx.send(AppEvent::BackgroundDownloadReady {
                                generation: task_gen,
                                temp_path,
                                options,
                            });
                        }
                        Err(e) => {
                            let _ = tx.send(AppEvent::BackgroundError {
                                generation: task_gen,
                                message: crate::error_display::user_message_from_report(&e, None),
                            });
                        }
                    },
                );
                None
            }
            #[cfg(any(feature = "http", feature = "cloud"))]
            AppEvent::BackgroundRemoteSizeReady {
                generation,
                pending,
            } => {
                if *generation != self.task_generation || !self.load_active {
                    return None;
                }
                self.status_message = None;
                self.confirmation_modal
                    .show(Self::download_confirmation_message(pending));
                self.pending_download = Some(((**pending).clone(), self.lease_the_generation()));
                None
            }
            #[cfg(any(feature = "http", feature = "cloud"))]
            AppEvent::BackgroundDownloadReady {
                generation,
                temp_path,
                options,
            } => {
                // `http_temp_path` below is the only thing that ever records this file
                // for cleanup, so a download we are not going to use has to remove it
                // here or it sits in the temp directory for good — and an abandoned
                // one can be gigabytes.
                if *generation != self.task_generation || !self.load_active {
                    let _ = std::fs::remove_file(temp_path);
                    return None;
                }
                self.http_temp_path = Some(temp_path.clone());
                // The URL the load was opened as, which is what opening it again names.
                if let LoadingState::Loading { file_path, .. } = &self.loading_state {
                    self.downloaded_from = file_path.clone();
                }
                if let LoadingState::Loading {
                    file_path,
                    file_size,
                    ..
                } = &self.loading_state
                {
                    self.loading_state = LoadingState::Loading {
                        file_path: file_path.clone(),
                        file_size: *file_size,
                        current_phase: "Scanning".to_string(),
                        progress_percent: 30,
                    };
                }
                self.status_message = Some("Scanning...".to_string());
                Some(AppEvent::DoLoadFromHttpTemp(
                    temp_path.clone(),
                    options.clone(),
                ))
            }
            #[cfg(any(feature = "http", feature = "cloud"))]
            AppEvent::DoLoadFromHttpTemp(temp_path, options) => {
                // Ahead of the `http_temp_path` assignment: an abandoned download is
                // ours to clean up, and nothing else records this file for removal.
                if !self.load_active {
                    let _ = std::fs::remove_file(temp_path);
                    return None;
                }
                self.http_temp_path = Some(temp_path.clone());
                // The URL the user typed, not the temp file it landed in.
                let display_path = match &self.loading_state {
                    LoadingState::Loading { file_path, .. } => file_path.clone(),
                    _ => None,
                };
                if let LoadingState::Loading {
                    file_path,
                    file_size,
                    ..
                } = &self.loading_state
                {
                    self.loading_state = LoadingState::Loading {
                        file_path: file_path.clone(),
                        file_size: *file_size,
                        current_phase: "Scanning".to_string(),
                        progress_percent: 30,
                    };
                }
                // A compressed CSV has to be decompressed before it can be scanned, as it
                // is when opened from disk; scanning the download directly read `.gz` as
                // a format and refused it.
                let compressed_csv = options
                    .compression
                    .or_else(|| CompressionFormat::from_extension(temp_path))
                    .is_some()
                    && (options.format == Some(FileFormat::Csv)
                        || temp_path
                            .file_stem()
                            .and_then(|stem| stem.to_str())
                            .is_some_and(|stem| stem.to_ascii_lowercase().ends_with(".csv")));
                if compressed_csv {
                    return Some(AppEvent::DoDecompress(
                        vec![temp_path.clone()],
                        options.clone(),
                    ));
                }
                self.spawn_scan_as(
                    "Scanning...",
                    vec![temp_path.clone()],
                    options.clone(),
                    display_path,
                )
            }
            AppEvent::DoLoadSchema(lf, path, options) => {
                if !self.load_active {
                    return None;
                }
                // Set "Caching schema" and return so the UI draws this phase before we block in DoLoadSchemaBlocking
                if let LoadingState::Loading {
                    file_path,
                    file_size,
                    ..
                } = &self.loading_state
                {
                    self.loading_state = LoadingState::Loading {
                        file_path: file_path.clone(),
                        file_size: *file_size,
                        current_phase: "Caching schema".to_string(),
                        progress_percent: 40,
                    };
                }
                Some(AppEvent::DoLoadSchemaBlocking(
                    lf.clone(),
                    path.clone(),
                    options.clone(),
                ))
            }
            AppEvent::DoLoadSchemaBlocking(lf, path, options) => {
                if !self.load_active {
                    return None;
                }
                self.debug.schema_load = None;
                let lf_owned = (**lf).clone();
                let path_owned = path.clone();
                let options_owned = options.clone();
                let schema_slot = self.pending_schema_result.clone();
                let cloud = self.app_config.cloud.clone();
                let runtime = self.runtime.clone();
                let report = crate::measurements::OpenReport {
                    progress: self.footer_progress.clone(),
                    meter: Arc::new(crate::measurements::Meter::default()),
                    remembered: Some(self.cache.clone()),
                };
                self.spawn_bg("Caching schema...", move |task_gen, tx| {
                    match Self::build_schema_state(
                        lf_owned,
                        path_owned.as_deref(),
                        &options_owned,
                        &cloud,
                        &runtime,
                        &report,
                    ) {
                        Ok((state, debug_label)) => {
                            let mut slot = schema_slot.lock().unwrap_or_else(|e| e.into_inner());
                            let dominated = slot.as_ref().is_some_and(|(g, _)| *g > task_gen);
                            if !dominated {
                                *slot = Some((task_gen, state));
                            }
                            drop(slot);
                            let _ = tx.send(AppEvent::BackgroundSchemaReady {
                                generation: task_gen,
                                path: path_owned,
                                options: options_owned,
                                debug_label: Some(debug_label),
                            });
                        }
                        Err(e) => {
                            let _ = tx.send(AppEvent::BackgroundError {
                                generation: task_gen,
                                message: crate::error_display::user_message_from_report(&e, None),
                            });
                        }
                    }
                });
                None
            }
            AppEvent::DoLoadBuffer => {
                if !self.load_active {
                    return None;
                }
                // No cleanup arm of its own. A collect asked for here is always owed
                // rather than run — the pump holds a lease for the whole of this handler
                // — so `collect_when_the_work_allows` is what finds out there is nothing
                // to collect, and it is the one that takes the loading screen down. Two
                // copies of that cleanup, one of them unreachable and less careful about
                // an export's `loading_state`, is an invitation to fix the wrong one.
                self.spawn_async_collect(Self::LOADING_BUFFER);
                None
            }
            AppEvent::DoDecompress(paths, options) => {
                if !self.load_active {
                    return None;
                }
                let path = paths[0].clone();
                // Only a CSV comes this way; said, so it can have its header turned off.
                let options_owned = OpenOptions {
                    format: options.format.or(Some(FileFormat::Csv)),
                    ..options.clone()
                };
                let schema_slot = self.pending_schema_result.clone();
                self.spawn_bg("Decompressing...", move |task_gen, tx| {
                    match Self::decompressed_csv_state(&path, &options_owned) {
                        Ok(state) => {
                            let mut slot = schema_slot.lock().unwrap_or_else(|e| e.into_inner());
                            let dominated = slot.as_ref().is_some_and(|(g, _)| *g > task_gen);
                            if !dominated {
                                *slot = Some((task_gen, state));
                            }
                            drop(slot);
                            let _ = tx.send(AppEvent::BackgroundSchemaReady {
                                generation: task_gen,
                                path: Some(path),
                                options: options_owned,
                                debug_label: Some("decompressed csv".to_string()),
                            });
                        }
                        Err(e) => {
                            let _ = tx.send(AppEvent::BackgroundError {
                                generation: task_gen,
                                message: crate::error_display::user_message_from_report(
                                    &e,
                                    Some(path.as_path()),
                                ),
                            });
                        }
                    }
                });
                None
            }
            AppEvent::Resize(_cols, _rows) => {
                // No work here: the next render sets visible_rows and flips needs_recollect,
                // which the main loop turns into an async collect against the correct size.
                None
            }
            AppEvent::Collect => {
                self.spawn_async_collect(Self::LOADING_BUFFER);
                None
            }
            AppEvent::DoScrollDown => self.handle_scroll(|s| s.page_down()),
            AppEvent::DoScrollUp => self.handle_scroll(|s| s.page_up()),
            AppEvent::DoScrollNext => self.handle_scroll(|s| s.select_next()),
            AppEvent::DoScrollPrev => self.handle_scroll(|s| s.select_previous()),
            AppEvent::DoScrollEnd => self.handle_scroll(|s| s.scroll_to_end()),
            AppEvent::DoScrollHome => self.handle_scroll(|s| s.scroll_to_start()),
            AppEvent::DoScrollHalfDown => self.handle_scroll(|s| s.half_page_down()),
            AppEvent::DoScrollHalfUp => self.handle_scroll(|s| s.half_page_up()),
            AppEvent::GoToLine(n) => {
                let n = *n;
                self.handle_scroll(|s| s.scroll_to_row_centered(n))
            }
            AppEvent::AnalysisChunk => {
                // Binary columns are stubbed by the source: their blobs are never read
                // for analysis (multi-GB blobs across partitions can exhaust memory).
                let (source, known_total) = match &self.data_table_state {
                    Some(state) => self.sample_source(state),
                    None => {
                        self.analysis_computation = None;
                        self.analysis_modal.computing = None;
                        self.busy = false;
                        return None;
                    }
                };
                let comp = self.analysis_computation.take()?;
                if comp.df.is_none() {
                    let sample = self.analysis_modal.sample.clone();
                    let streaming = self.app_config.performance.polars_streaming;
                    self.spawn_bg("Computing statistics...", move |task_gen, tx| match source
                        .cut(&sample.scope)
                        .and_then(|lf| {
                            crate::statistics::compute_describe_from_lazy(
                                &lf,
                                known_total,
                                &sample,
                                streaming,
                            )
                        }) {
                        Ok(results) => {
                            let _ = tx.send(AppEvent::BackgroundDescribeReady {
                                generation: task_gen,
                                results,
                            });
                        }
                        Err(e) => {
                            let _ = tx.send(AppEvent::BackgroundError {
                                generation: task_gen,
                                message: format!("{e}"),
                            });
                        }
                    });
                }
                None
            }
            AppEvent::AnalysisDistributionCompute => {
                if let Some(state) = &self.data_table_state {
                    let (source, known_total) = self.sample_source(state);
                    let sample = self.analysis_modal.sample.clone();
                    let streaming = self.app_config.performance.polars_streaming;
                    self.spawn_bg("Analyzing distributions...", move |task_gen, tx| {
                        let options = crate::statistics::ComputeOptions {
                            include_distribution_info: true,
                            include_distribution_analyses: true,
                            include_correlation_matrix: false,
                            include_skewness_kurtosis_outliers: true,
                            polars_streaming: streaming,
                        };
                        match source.cut(&sample.scope).and_then(|lf| {
                            crate::statistics::compute_statistics_for_sample(
                                &lf,
                                &sample,
                                known_total,
                                options,
                            )
                        }) {
                            Ok(results) => {
                                let _ = tx.send(AppEvent::BackgroundDistributionReady {
                                    generation: task_gen,
                                    results,
                                });
                            }
                            Err(e) => {
                                let _ = tx.send(AppEvent::BackgroundError {
                                    generation: task_gen,
                                    message: format!("{e}"),
                                });
                            }
                        }
                    });
                } else {
                    self.analysis_modal.computing = None;
                    self.busy = false;
                }
                None
            }
            AppEvent::AnalysisCorrelationCompute => {
                if let Some(state) = &self.data_table_state {
                    let (source, known_total) = self.sample_source(state);
                    let streaming = state.polars_streaming;
                    let sample = self.analysis_modal.sample.clone();
                    let seed = sample.seed;
                    self.spawn_bg("Computing correlation matrix...", move |task_gen, tx| {
                        // Only the numeric columns: nothing else is correlated, and on a
                        // wide table the rest is most of what a full read would hold.
                        let result = source
                            .cut(&sample.scope)
                            .and_then(|lf| {
                                let schema = lf.clone().collect_schema()?;
                                let numeric: Vec<polars::prelude::Expr> = schema
                                    .iter()
                                    .filter(|(_, dtype)| dtype.is_numeric())
                                    .map(|(name, _)| col(name.clone()))
                                    .collect();
                                crate::sampling::read(
                                    &lf.select(numeric),
                                    &sample,
                                    known_total,
                                    streaming,
                                )
                            })
                            .map(|rows| {
                                let matrix =
                                    crate::statistics::compute_correlation_matrix(&rows.df).ok();
                                crate::statistics::AnalysisResults {
                                    column_statistics: vec![],
                                    total_rows: rows.total_rows,
                                    sample_size: rows.sample_size,
                                    per_value: rows.per_value.map(|per_value| per_value.kept),
                                    sample_seed: seed,
                                    correlation_matrix: matrix,
                                    distribution_analyses: vec![],
                                }
                            });
                        match result {
                            Ok(results) => {
                                let _ = tx.send(AppEvent::BackgroundCorrelationReady {
                                    generation: task_gen,
                                    results,
                                });
                            }
                            Err(e) => {
                                let _ = tx.send(AppEvent::BackgroundError {
                                    generation: task_gen,
                                    message: format!("{e}"),
                                });
                            }
                        }
                    });
                } else {
                    self.analysis_modal.computing = None;
                    self.busy = false;
                }
                None
            }
            AppEvent::AnalysisDataQualityCompute => {
                // Whatever path asked for the run, it reads the shared sample.
                self.sync_quality_plan();
                if let Some(state) = &self.data_table_state {
                    let plan = self.analysis_modal.data_quality_plan.clone();
                    let source_scope = plan.scope.uses_source();
                    let (lf, source, cached_rows) = if source_scope {
                        let (lf, source) = state.data_quality_source_scan();
                        (lf, source, None)
                    } else {
                        let ordered = matches!(
                            plan.scope,
                            data_quality::QualityScope::FirstRows(_)
                                | data_quality::QualityScope::ViewRows { .. }
                        );
                        let (lf, source) = state.data_quality_scan(ordered);
                        let rows = state.num_rows_if_valid().map(|rows| match &plan.scope {
                            data_quality::QualityScope::CurrentView => rows,
                            data_quality::QualityScope::FirstRows(limit) => rows.min(*limit),
                            data_quality::QualityScope::ViewRows { start, end } => {
                                rows.min(*end).saturating_sub(start.saturating_sub(1))
                            }
                            _ => unreachable!(),
                        });
                        (lf, source, rows)
                    };
                    let streaming = state.polars_streaming;
                    let view_generation = state.len_generation();
                    let dataset_generation = self.dataset_generation;
                    let kept = self.kept_quality_sample(&plan.sample());
                    // Only a confirmed full scan pays to read the values a type
                    // conflict hides, and only its access plan promised the read.
                    let mut source = source;
                    if plan.compute == data_quality::QualityCompute::Full
                        && let Some(source) = source.as_mut()
                    {
                        source.conflict_scan = state.quality_conflict_scan();
                    }
                    self.spawn_bg("Profiling data quality...", move |task_gen, tx| {
                        let lf = if source_scope {
                            match data_quality::prepare_source_quality_scan(lf, source.as_ref()) {
                                Ok(lf) => lf,
                                Err(error) => {
                                    let _ = tx.send(AppEvent::BackgroundError {
                                        generation: task_gen,
                                        message: format!("{error}"),
                                    });
                                    return;
                                }
                            }
                        } else {
                            lf
                        };
                        let lf = match data_quality::apply_quality_scope(
                            lf,
                            &plan.scope,
                            source.as_ref(),
                        ) {
                            Ok(lf) => lf,
                            Err(error) => {
                                let _ = tx.send(AppEvent::BackgroundError {
                                    generation: task_gen,
                                    message: format!("{error}"),
                                });
                                return;
                            }
                        };
                        match crate::data_quality::compute_data_quality_kept(
                            &lf,
                            cached_rows,
                            &plan,
                            source.as_ref(),
                            streaming,
                            kept.as_deref(),
                        ) {
                            Ok((results, rows)) => {
                                let _ = tx.send(AppEvent::BackgroundDataQualityReady {
                                    generation: task_gen,
                                    results,
                                    kept: rows.map(|rows| KeptQualitySample {
                                        dataset_generation,
                                        view_generation,
                                        sample: plan.sample(),
                                        rows: std::sync::Arc::new(rows),
                                    }),
                                });
                            }
                            Err(error) => {
                                let _ = tx.send(AppEvent::BackgroundError {
                                    generation: task_gen,
                                    message: format!("{error}"),
                                });
                            }
                        }
                    });
                } else {
                    self.analysis_modal.computing = None;
                    self.busy = false;
                }
                None
            }
            AppEvent::BackgroundLenReady {
                len_generation,
                num_rows,
                file_row_groups,
            } => {
                if self.len_count_inflight == Some(*len_generation) {
                    self.len_count_inflight = None;
                }
                if self.len_count_failed == Some(*len_generation) {
                    self.len_count_failed = None;
                }
                // Apply the exact total only if the data hasn't changed since the count
                // was spawned. This runs independently of the buffer paint (which has
                // usually already rendered), so it just corrects the scrollbar/total —
                // no busy state, no re-collect.
                if let Some(state) = self.data_table_state.as_mut()
                    && state.len_generation() == *len_generation
                {
                    match file_row_groups {
                        Some(groups) => state.set_file_row_groups(groups),
                        None => state.set_num_rows(*num_rows),
                    }
                    // End was pressed before there was an end to go to.
                    if self.end_after_count == Some(*len_generation) {
                        self.end_after_count = None;
                        self.status_message = None;
                        return self.jump_key(AppEvent::DoScrollEnd);
                    }
                } else if self.end_after_count == Some(*len_generation) {
                    // This is the count End was waiting on, and it answers a frame that
                    // is gone — a join landed underneath it and took a fresh
                    // `len_generation` past it. Left here the flag is stranded on a
                    // generation nothing will ever match: the next count to fail for any
                    // reason would speak in its name. So it is retired, and the status
                    // it put up comes down with it.
                    //
                    // Retired, not asked again of the frame that is here. That frame can
                    // belong to a dataset the user opened since — `end_after_count` names
                    // a `len_generation`, which says nothing about which dataset — and
                    // re-asking made the *new* dataset scroll itself to the end on the
                    // strength of a key pressed in the old one. A jump the frame change
                    // swallowed is a jump the user can make again; a jump that arrives on
                    // its own, in a directory they did not press it in, is not.
                    self.retire_the_end_that_was_waiting();
                }
                None
            }
            AppEvent::BackgroundLenFailed { len_generation } => {
                if self.len_count_inflight == Some(*len_generation) {
                    self.len_count_inflight = None;
                }
                // Mark this generation's count as failed so the row count renders as "?"
                // instead of a misleading provisional total. Before the End handling
                // below: this is about the count, not about who was waiting on it.
                //
                // Only for the frame on screen, because the slot holds one generation.
                // Counts for two frames run at once — a join, a query, a filter or a
                // sort takes a fresh `len_generation` without stopping the count already
                // running — so a failure arriving is not necessarily this frame's.
                // Written unconditionally, an orphan's failure overwrote a live frame's,
                // `count_unknown` went false, and the bar fell through from "?" to the
                // number the buffer happened to reach: a confident partial on a dataset
                // whose count failed. The orphan's own failure is worth nothing to
                // anybody — nothing will ever render against a generation that is gone.
                if self
                    .data_table_state
                    .as_ref()
                    .is_some_and(|state| state.len_generation() == *len_generation)
                {
                    self.len_count_failed = Some(*len_generation);
                }
                // Only for the count End was actually waiting on. Taken unconditionally,
                // a count that failed for one frame answered for an End pressed on
                // another — printing "Could not count the rows to find the end" about a
                // key the user pressed somewhere else entirely, and long since.
                if self.end_after_count == Some(*len_generation) {
                    self.end_after_count = None;
                    if self
                        .data_table_state
                        .as_ref()
                        .is_some_and(|state| state.len_generation() == *len_generation)
                    {
                        self.status_message =
                            Some("Could not count the rows to find the end".to_string());
                    } else {
                        // The frame it was counting is gone, so its failure says nothing
                        // about the one on screen, and the End it belonged to cannot be
                        // answered by it. Retired quietly, as above.
                        self.take_down_the_counting_status();
                    }
                }
                None
            }
            AppEvent::BackgroundCollectReady { generation } => {
                if *generation == self.task_generation {
                    // A load-ahead's end is nobody's wait ending: whatever else is busy
                    // meanwhile keeps its throbber and its message.
                    let waited_on = self
                        .collect_inflight
                        .as_ref()
                        .is_none_or(|inflight| inflight.waited_on);
                    // Timed to here rather than to the next paint: this is the moment
                    // the rows exist to be drawn, and the frame that draws them costs
                    // the same whatever the page cost to fetch.
                    if let Some(inflight) = self.collect_inflight.take()
                        && let Some(state) = self.data_table_state.as_ref()
                    {
                        state
                            .measurements()
                            .read_page(inflight.began.elapsed(), inflight.files);
                    }
                    let taken = self
                        .pending_collect_result
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .take();
                    if let Some((slot_gen, result)) = taken
                        && slot_gen == self.task_generation
                        && let Some(state) = &mut self.data_table_state
                    {
                        state.apply_async_collect(result);
                    }
                    if waited_on {
                        self.loading_state = LoadingState::Idle;
                        self.status_message = None;
                        self.busy = false;
                    }
                }
                // Stale results (generation mismatch) are silently ignored —
                // busy stays true until the current generation's result arrives.
                None
            }
            AppEvent::BackgroundCollectFailed {
                generation,
                message,
            } => {
                if *generation != self.task_generation {
                    return None;
                }
                let waited_on = self
                    .collect_inflight
                    .as_ref()
                    .is_none_or(|inflight| inflight.waited_on);
                if waited_on {
                    return Some(AppEvent::BackgroundError {
                        generation: *generation,
                        message: message.clone(),
                    });
                }
                self.collect_inflight = None;
                None
            }
            AppEvent::BackgroundFootersJoined { .. } => {
                // Taken whoever the event belongs to, and judged by what is *in* the
                // slot rather than by the event that woke us. Two passes can be running
                // at once, and the newer one may have overwritten the slot before the
                // older one's event is handled: judging by the event would throw the
                // newer answer away and leave the dataset on screen waiting for one
                // that has already been and gone. An entry is also worth draining
                // either way — it is a dataset's worth of schema and every file name.
                let taken = self
                    .pending_footers_result
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take();
                // Whether this is still the dataset on screen. Not `load_active`: going
                // home leaves the dataset up and clears that flag, and coming straight
                // back to it must not find it stranded on two footers for the rest of
                // the session.
                if let Some((slot_generation, found)) = taken
                    && slot_generation == self.dataset_generation
                {
                    let Some(found) = found else {
                        // The pass could not read them. The dataset stays as it opened
                        // and stops waiting, so it can go and count itself the ordinary
                        // way rather than never at all — which is what the collect
                        // below sets going, since it is the counting the dataset was
                        // declining while it waited.
                        if let Some(state) = self.data_table_state.as_mut() {
                            state.give_up_on_pending_footers();
                        }
                        // The pass is not bringing a count after all, so the jump goes
                        // back to waiting on the ordinary one the collect starts. Owed
                        // rather than run: the collect bumps `task_generation`, and an
                        // export or an analysis may be waiting on the one it would bump
                        // past. `reread_when_the_work_allows` runs it the moment that
                        // work is done.
                        self.reread_owed = Some(slot_generation);
                        self.reread_when_the_work_allows();
                        return None;
                    };
                    self.footers_held = Some((slot_generation, found));
                    if self.join_held_footers() {
                        self.reread_after_the_footers_joined();
                    }
                }
                None
            }
            AppEvent::BackgroundSchemaReady {
                generation,
                path,
                options,
                debug_label,
            } => {
                // `load_active` also gates the "loading failed silently" reset below:
                // an abandoned load must not clear busy/loading state that a newer
                // load, or an export, may already own.
                if *generation == self.task_generation && self.load_active {
                    let taken = self
                        .pending_schema_result
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .take();
                    if let Some((slot_gen, state)) = taken
                        && slot_gen == self.task_generation
                    {
                        self.apply_schema_ready(state, path.clone(), options, debug_label.clone());
                        return Some(AppEvent::DoLoadBuffer);
                    }
                    // Generation matched but slot was empty or stale — loading failed silently.
                    self.awaiting_dataset = false;
                    self.recent_on_install = None;
                    self.opening = None;
                    self.loading_state = LoadingState::Idle;
                    self.status_message = None;
                    self.busy = false;
                }
                // Stale message (generation mismatch) — ignore entirely.
                None
            }
            AppEvent::BackgroundSampleReady {
                generation,
                df,
                label,
            } => {
                if *generation == self.task_generation {
                    self.analysis_modal.computing = None;
                    self.reading_sample = false;
                    self.status_message = None;
                    self.busy = false;
                    self.show_sample_view(df.clone(), label.clone());
                }
                None
            }
            AppEvent::BackgroundDescribeReady {
                generation,
                results,
            } => {
                if *generation == self.task_generation {
                    self.analysis_modal.describe_results = Some(results.clone());
                    self.analysis_modal.computing = None;
                    self.status_message = None;
                    self.busy = false;
                }
                None
            }
            AppEvent::BackgroundDistributionReady {
                generation,
                results,
            } => {
                if *generation == self.task_generation {
                    self.analysis_modal.distribution_results = Some(results.clone());
                    self.analysis_modal.computing = None;
                    self.status_message = None;
                    self.busy = false;
                }
                None
            }
            AppEvent::BackgroundCorrelationReady {
                generation,
                results,
            } => {
                if *generation == self.task_generation {
                    self.analysis_modal.correlation_results = Some(results.clone());
                    self.analysis_modal.computing = None;
                    self.status_message = None;
                    self.busy = false;
                }
                None
            }
            AppEvent::BackgroundDataQualityReady {
                generation,
                results,
                kept,
            } => {
                // Kept whatever became of the run's results: the rows are the rows the
                // key names, and a read is not to be thrown away.
                if let Some(kept) = kept
                    && kept.dataset_generation == self.dataset_generation
                {
                    self.quality_sample = Some(kept.clone());
                }
                if *generation == self.task_generation
                    && self.analysis_modal.active
                    && self.analysis_modal.selected_tool
                        == Some(analysis_modal::AnalysisTool::DataQuality)
                {
                    self.cache_quality_result(
                        results,
                        self.analysis_modal.data_quality_plan.clone(),
                    );
                    self.analysis_modal.data_quality_last_plan =
                        Some(self.analysis_modal.data_quality_plan.clone());
                    self.analysis_modal.data_quality_results = Some(results.clone());
                    self.analysis_modal.data_quality_from_cache = false;
                    self.analysis_modal
                        .set_quality_page(crate::data_quality::QualityPage::Overview);
                    self.analysis_modal.computing = None;
                    self.status_message = None;
                    self.busy = false;
                }
                None
            }
            AppEvent::BackgroundExportCollected {
                generation,
                df,
                path,
                format,
                options,
            } => {
                if *generation == self.task_generation {
                    // The frame was collected with the scan's row index still on it
                    // when the export asked to name each row's file; swap it for the
                    // names. Where that cannot be done the export goes ahead without
                    // the column — but the index still has to come off, or datui's own
                    // bookkeeping lands in the user's file.
                    let df = match self
                        .data_table_state
                        .as_ref()
                        .filter(|state| options.source_file && state.can_name_source_files())
                        .map(|state| state.name_source_files(df.clone()))
                    {
                        Some(Ok(named)) => named,
                        _ => DataTableState::drop_row_index(df.clone()),
                    };
                    self.export_df = Some(df);
                    let has_compression = match format {
                        ExportFormat::Csv => options.csv_compression.is_some(),
                        ExportFormat::Json => options.json_compression.is_some(),
                        ExportFormat::Ndjson => options.ndjson_compression.is_some(),
                        ExportFormat::Parquet | ExportFormat::Ipc | ExportFormat::Avro => false,
                    };
                    let phase = if has_compression {
                        "Writing and compressing file"
                    } else {
                        "Writing file"
                    };
                    self.loading_state = LoadingState::Exporting {
                        file_path: path.clone(),
                        current_phase: phase.to_string(),
                        progress_percent: 50,
                    };
                    self.status_message = Some(format!("{}...", phase));
                    return Some(AppEvent::DoExportWrite(
                        path.clone(),
                        *format,
                        options.clone(),
                    ));
                }
                // Stale export collect — ignore.
                None
            }
            AppEvent::BackgroundExportWritten {
                generation,
                path,
                result,
            } => {
                if *generation == self.task_generation {
                    self.loading_state = LoadingState::Idle;
                    self.status_message = None;
                    self.busy = false;
                    match result {
                        Ok(()) => {
                            self.flash_note(format!("Exported to {}", path.display()));
                        }
                        Err(e) => {
                            self.error_modal.show(e.clone());
                        }
                    }
                }
                None
            }
            AppEvent::LookThenOpenDirectory(dir, options) => {
                // The name on the wait, so the first frame says which directory is being
                // looked at rather than sitting blank. `spawn_bg` puts the throbber up
                // and the keys that survive it — Ctrl+C, Ctrl+O — keep working, which
                // is the whole of what doing this on the event thread cost.
                let looking = dir.clone();
                let options = options.clone();
                self.set_loading_phase(Self::LOOKING_AT_A_DIRECTORY, 5);
                self.name_what_is_loading(looking.clone());
                self.looking_at_directory = Some(looking.clone());
                // The same words the loading screen shows, so the control bar and the
                // screen above it do not name the wait two different ways.
                // Unleased. A lease exists to make a bump wait for an answer that
                // would otherwise be stranded — and this answer is *meant* to be
                // thrown away when the user moves on, which is the whole of the guard
                // below. Leased, it made everything else wait instead: Ctrl+O out of a
                // seventeen-second look and open a small CSV, and its buffer collect
                // parks in `collect_owed` until the abandoned look finally returns.
                // Advertising Ctrl+O as the way out of the wait and then holding the
                // next dataset behind it is the wait again, wearing a different hat.
                #[cfg(feature = "cloud")]
                let (cloud, runtime) = (self.app_config.cloud.clone(), self.runtime.clone());
                self.spawn_bg_replaceable(
                    Some(Self::LOOKING_AT_A_DIRECTORY),
                    move |task_gen, tx| {
                        #[cfg(feature = "cloud")]
                        if home::is_object_store_url(&looking) {
                            let url = looking.to_string_lossy().into_owned();
                            let peeked = wait_on_runtime(&runtime, async move {
                                crate::cloud_browse::peek_kind(&url, &cloud).await
                            })
                            .and_then(Result::ok);
                            let (kind, holds) = match peeked {
                                Some((kind, holds)) => (kind, Some(Box::new(holds))),
                                None => (discover::EntryKind::Unknown, None),
                            };
                            let _ = tx.send(AppEvent::DirectoryLookedAt {
                                generation: task_gen,
                                path: looking,
                                kind,
                                holds,
                                options: Box::new(options),
                            });
                            return;
                        }
                        // A panic here used to unwind through `run()` and report a crash,
                        // because the look was made on the way to the first frame. On a
                        // worker it is swallowed with the dropped handle instead, and nothing
                        // would ever be sent: the spinner would stay up and the directory
                        // unopened for as long as the user waited. Caught, so the answer is
                        // "a directory" and the home screen opens on it. Read the way this
                        // open will read them, so the rule judges the directory the user is
                        // about to see rather than one nobody will open.
                        let as_read = Self::read_as(&options);
                        let looked = std::panic::catch_unwind(|| {
                            let mut entry = discover::Entry::directory(&looking);
                            entry.kind = discover::EntryKind::Unknown;
                            home::look_into_as(&entry, &as_read)
                        });
                        let kind = match looked {
                            Ok(entry) => entry.kind,
                            Err(_) => discover::EntryKind::Directory,
                        };
                        let _ = tx.send(AppEvent::DirectoryLookedAt {
                            generation: task_gen,
                            path: looking,
                            kind,
                            holds: None,
                            options: Box::new(options),
                        });
                    },
                );
                None
            }
            AppEvent::DirectoryLookedAt {
                generation,
                path,
                kind,
                holds,
                options,
            } => {
                // The user pressed Ctrl+O and went to the home screen, or opened
                // something else, while this was reading. Their choice is the one on
                // screen, and this is the answer to a question nobody is waiting for.
                //
                // Both tests: the directory, because a newer look replaces an older one,
                // and the generation, because other work bumps that when it takes the
                // screen over.
                // Not ours: a newer look is out and this is an older answer, so the
                // tracking belongs to that one and is left alone. Taking it here would
                // strand the newer answer as unowned, and the app would sit on a
                // loading screen with nothing left to clear it.
                if self.looking_at_directory.as_deref() != Some(path.as_path()) {
                    return None;
                }
                // Ours, so it is put down whatever happens next — including the
                // generation test below. Left set, the next `abandon_load` from
                // anywhere would find it and clear `busy` for work it does not own.
                self.looking_at_directory = None;
                if *generation != self.task_generation {
                    return None;
                }
                let outcome = self.open_the_directory_looked_at(
                    path.clone(),
                    *kind,
                    holds.as_deref(),
                    (**options).clone(),
                );
                // Only when nothing follows. An `Open` keeps the wait up — it sets its
                // own phase and `busy` — and clearing them here would draw one frame
                // with the spinner stopped and the keys held during the look replayed
                // into an app that has no dataset yet, ahead of the load.
                if outcome.is_none() {
                    self.busy = false;
                    if self.status_message.as_deref() == Some(Self::LOOKING_AT_A_DIRECTORY) {
                        self.status_message = None;
                    }
                }
                outcome
            }
            AppEvent::ClassifyThenOpen { path, jump } => {
                // A second Enter replaces the first rather than being refused. Every key
                // acts on the home screen even while `busy`, so a second one is
                // reachable, and the newer look is the one the user is waiting for — and
                // refusing meant a look at a share that never answers killed the feature
                // for the rest of the session, silently.
                let looking = path.clone();
                let jump = *jump;
                self.classify_requests = self.classify_requests.wrapping_add(1);
                let request = self.classify_requests;
                self.classify_inflight = Some(ClassifyRequest {
                    id: request,
                    path: looking.clone(),
                    browsing: self.home.browsing.clone(),
                });
                let name = looking
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|| looking.display().to_string());
                // The home screen's own line, because the control bar's is the table's.
                self.home.status = Some(format!("Looking at {name}..."));
                self.spawn_bg(Self::LOOKING, move |task_gen, tx| {
                    // Every one of these can sit forever on a share that has gone away,
                    // which is the whole reason they are here and not where keys are read.
                    let found = if !looking.exists() {
                        None
                    } else if looking.is_dir() {
                        Some(crate::discover::classify_directory(&looking))
                    } else {
                        Some(crate::discover::EntryKind::File)
                    };
                    let _ = tx.send(AppEvent::BackgroundKindReady {
                        generation: task_gen,
                        request,
                        path: looking,
                        found,
                        jump,
                    });
                });
                None
            }
            AppEvent::DrillRowRead {
                generation,
                group_index,
                row,
            } => {
                // A bump means something replaced the view; its owner has the busy state.
                if *generation != self.task_generation {
                    return None;
                }
                self.busy = false;
                if self.status_message.as_deref() == Some(Self::READING_GROUP) {
                    self.status_message = None;
                }
                match row {
                    Ok(row) => self.drill_into(*group_index, row),
                    Err(message) => self.flash_note(format!("Could not drill in: {message}")),
                }
                None
            }
            AppEvent::BackgroundKindReady {
                generation,
                request,
                path,
                found,
                jump,
            } => {
                // Superseded, or belonging to nothing: a newer look owns the busy state
                // and the status line, so this one touches neither.
                if self
                    .classify_inflight
                    .as_ref()
                    .map(|r| (r.id, r.path.as_path()))
                    != Some((*request, path.as_path()))
                {
                    return None;
                }
                let asked = self.classify_inflight.take().expect("just matched");
                if self.status_message.as_deref() == Some(Self::LOOKING) {
                    self.status_message = None;
                }
                self.home.status = None;

                // Something else took the busy state over. A bump comes from an `Open` or
                // a collect, and both set `busy` themselves — clearing it here would take
                // the throbber off a load still running and let keys land on a table
                // being replaced. Their answer, their busy.
                if *generation != self.task_generation {
                    return None;
                }
                // Otherwise it is this look's, and goes down however the answer lands.
                self.busy = false;

                // A key pressed on the home screen answers on the home screen. If they
                // went back to the data, opening now would arrive from nowhere; if the
                // browse has moved, the answer is about somewhere they navigated away
                // from, and acting on it would take them back into it.
                if self.input_mode != InputMode::Home || self.home.browsing != asked.browsing {
                    return None;
                }

                let Some(kind) = *found else {
                    self.home.status = Some(format!("No such path: {}", path.display()));
                    if *jump {
                        // A typo typed at `~` is worth another go without retyping it.
                        self.home.path_input = path.display().to_string();
                        self.home.path_input_active = true;
                    }
                    return None;
                };
                self.open_what_it_is(path.clone(), kind, *jump)
            }
            AppEvent::BackgroundWorkFinished { generation } => {
                // Behind the result its work sent, so the handler that consumed that
                // result has already run. Saturating because a lease released twice
                // would otherwise wrap into "nothing is ever safe to bump".
                if let Some(n) = self.leases.get_mut(generation) {
                    *n = n.saturating_sub(1);
                    if *n == 0 {
                        self.leases.remove(generation);
                    }
                }
                None
            }
            AppEvent::BackgroundError {
                generation,
                message,
            } => {
                if *generation == self.task_generation {
                    self.collect_inflight = None;
                    self.analysis_modal.computing = None;
                    // A load chosen at home fails at home, with the reason beside the
                    // prompt once the modal is gone. Otherwise the previous dataset is
                    // the current one again, and it is what the error modal sits over.
                    let back_home = self.awaiting_dataset && self.load_from_home;
                    self.load_from_home = false;
                    self.recent_on_install = None;
                    self.opening = None;
                    self.awaiting_dataset = false;
                    self.loading_state = LoadingState::Idle;
                    self.status_message = None;
                    self.busy = false;
                    // Kept so the home screen can say why, if that is where dismissing
                    // the error lands the user.
                    self.last_load_error = Some(message.clone());
                    if back_home {
                        self.enter_home();
                        self.home.status = self.last_load_error.clone();
                    }
                    self.error_modal.show(message.clone());
                }
                None
            }
            AppEvent::Search(query) => {
                let query_succeeded = if let Some(state) = &mut self.data_table_state {
                    state.defer_collect = true;
                    state.query(query.clone());
                    state.defer_collect = false;
                    state.error.is_none()
                } else {
                    false
                };

                if query_succeeded {
                    self.input_mode = InputMode::Normal;
                    self.input_type = None;
                    self.query_input.set_focused(false);
                    if let Some(state) = &mut self.data_table_state {
                        state.suppress_error_display = false;
                    }
                    self.spawn_async_collect("Applying query...");
                }
                None
            }
            AppEvent::SqlSearch(sql) => {
                let sql_succeeded = if let Some(state) = &mut self.data_table_state {
                    state.sql_query(sql.clone());
                    state.error.is_none()
                } else {
                    false
                };
                if sql_succeeded {
                    self.input_mode = InputMode::Normal;
                    self.input_type = None;
                    self.sql_input.set_focused(false);
                    if let Some(state) = &mut self.data_table_state {
                        state.suppress_error_display = false;
                    }
                    self.spawn_async_collect("Applying SQL query...");
                }
                None
            }
            AppEvent::FuzzySearch(query) => {
                let fuzzy_succeeded = if let Some(state) = &mut self.data_table_state {
                    state.defer_collect = true;
                    state.fuzzy_search(query.clone());
                    state.defer_collect = false;
                    state.error.is_none()
                } else {
                    false
                };
                if fuzzy_succeeded {
                    self.input_mode = InputMode::Normal;
                    self.input_type = None;
                    self.fuzzy_input.set_focused(false);
                    if let Some(state) = &mut self.data_table_state {
                        state.suppress_error_display = false;
                    }
                    self.spawn_async_collect("Searching...");
                }
                None
            }
            AppEvent::Filter(statements) => {
                if let Some(state) = &mut self.data_table_state {
                    state.defer_collect = true;
                    state.filter(statements.clone());
                    state.defer_collect = false;
                }
                self.spawn_async_collect("Filtering...");
                None
            }
            AppEvent::Sort(columns, descending) => {
                if let Some(state) = &mut self.data_table_state {
                    state.defer_collect = true;
                    state.sort_by(columns.clone(), descending.clone());
                    state.defer_collect = false;
                }
                self.spawn_async_collect("Sorting...");
                None
            }
            AppEvent::Reset => {
                if let Some(state) = &mut self.data_table_state {
                    state.defer_collect = true;
                    state.reset();
                    state.defer_collect = false;
                }
                self.spawn_async_collect(Self::LOADING_BUFFER);
                // Clear active template when resetting
                self.active_template_id = None;
                None
            }
            AppEvent::ColumnOrder(order, locked_count) => {
                if let Some(state) = &mut self.data_table_state {
                    state.set_column_order(order.clone());
                    state.set_locked_columns(*locked_count);
                }
                None
            }
            AppEvent::Pivot(spec) => {
                self.busy = true;
                if let Some(state) = &mut self.data_table_state {
                    state.defer_collect = true;
                    let result = state.pivot(spec);
                    state.defer_collect = false;
                    match result {
                        Ok(()) => {
                            self.pivot_melt_modal.close();
                            self.input_mode = InputMode::Normal;
                            self.spawn_async_collect("Computing pivot...");
                            None
                        }
                        Err(e) => {
                            self.busy = false;
                            self.error_modal
                                .show(crate::error_display::user_message_from_report(&e, None));
                            None
                        }
                    }
                } else {
                    self.busy = false;
                    None
                }
            }
            AppEvent::Melt(spec) => {
                self.busy = true;
                if let Some(state) = &mut self.data_table_state {
                    state.defer_collect = true;
                    let result = state.melt(spec);
                    state.defer_collect = false;
                    match result {
                        Ok(()) => {
                            self.pivot_melt_modal.close();
                            self.input_mode = InputMode::Normal;
                            self.spawn_async_collect("Computing melt...");
                            None
                        }
                        Err(e) => {
                            self.busy = false;
                            self.error_modal
                                .show(crate::error_display::user_message_from_report(&e, None));
                            None
                        }
                    }
                } else {
                    self.busy = false;
                    None
                }
            }
            AppEvent::ChartExport(path, format, title, width, height) => {
                self.busy = true;
                self.loading_state = LoadingState::Exporting {
                    file_path: path.clone(),
                    current_phase: "Exporting chart".to_string(),
                    progress_percent: 0,
                };
                Some(AppEvent::DoChartExport(
                    path.clone(),
                    *format,
                    title.clone(),
                    *width,
                    *height,
                ))
            }
            AppEvent::DoChartExport(path, format, title, width, height) => {
                // `ChartExport` arms `busy` and defers here so the phase can be drawn
                // first. A Ctrl-O in that window has already left the chart view, and
                // there is nothing to export any more: release the app rather than park
                // an export that no view would ever prepare.
                if self.input_mode != InputMode::Chart || !self.chart_modal.active {
                    self.loading_state = LoadingState::Idle;
                    self.status_message = None;
                    self.busy = false;
                    return None;
                }
                self.start_chart_export(path.clone(), *format, title.clone(), *width, *height);
                None
            }
            AppEvent::BackgroundChartExportWritten {
                generation,
                path,
                format,
                result,
            } => {
                // Gated on the chart export's own marker, which leaving the dataset
                // clears: a write that finishes after Ctrl-O must not reopen its modal
                // over the home screen.
                if self.chart_export_inflight == Some(*generation) {
                    self.finish_chart_export(path, *format, result.clone());
                }
                None
            }
            AppEvent::BackgroundChartReady => {
                // The result belongs to the one preparation in flight. It is installed
                // only while that record is current (a reset marks it stale when its
                // view or dataset goes) and only into the dataset it was computed from.
                // Taking the record is what lets the next request start; the slot is
                // emptied either way so a discarded series is not kept around.
                let inflight = self.chart_inflight.take()?;
                let outcome = self
                    .pending_chart_result
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take()
                    .unwrap_or_else(|| Err("Chart preparation produced no result".to_string()));
                if inflight.stale {
                    return None;
                }
                let dataset = self.data_table_state.as_ref().map(|s| s.len_generation());
                if dataset != inflight.dataset {
                    return None;
                }
                self.chart_cache.insert(inflight.request, outcome);
                // An export parked on chart data resumes against the *current*
                // selection, whatever just landed: it is written if that selection is
                // now prepared, fails with the reason if that is the one that failed,
                // and otherwise waits for the next result (which `ensure_chart_data`
                // starts once this handler returns).
                if let Some((path, format, title, width, height)) = self.chart_export_waiting.take()
                {
                    self.start_chart_export(path, format, title, width, height);
                }
                None
            }
            AppEvent::Export(path, format, options) => {
                if let Some(_state) = &self.data_table_state {
                    self.busy = true;
                    // Show progress immediately
                    self.loading_state = LoadingState::Exporting {
                        file_path: path.clone(),
                        current_phase: "Preparing export".to_string(),
                        progress_percent: 0,
                    };
                    // Return DoExport to allow UI to render progress before blocking
                    Some(AppEvent::DoExport(path.clone(), *format, options.clone()))
                } else {
                    None
                }
            }
            AppEvent::DoExport(path, format, options) => {
                if let Some(_state) = &self.data_table_state {
                    // Phase 1: show "Collecting data" so UI can redraw before blocking collect
                    self.loading_state = LoadingState::Exporting {
                        file_path: path.clone(),
                        current_phase: "Collecting data".to_string(),
                        progress_percent: 10,
                    };
                    Some(AppEvent::DoExportCollect(
                        path.clone(),
                        *format,
                        options.clone(),
                    ))
                } else {
                    self.busy = false;
                    None
                }
            }
            AppEvent::DoExportCollect(path, format, options) => {
                if let Some(state) = &self.data_table_state {
                    // Naming each row's file needs the scan's row index, which
                    // `visible_lf` drops; the index is replaced by the name below.
                    let name_files = options.source_file && state.can_name_source_files();
                    let lf = if name_files {
                        state.lf_clone()
                    } else {
                        state.visible_lf()
                    };
                    let streaming = state.polars_streaming;
                    let path = path.clone();
                    let format = *format;
                    let options = options.clone();
                    self.spawn_bg("Collecting data for export...", move |task_gen, tx| {
                        match crate::statistics::collect_lazy(lf, streaming) {
                            Ok(df) => {
                                let _ = tx.send(AppEvent::BackgroundExportCollected {
                                    generation: task_gen,
                                    df,
                                    path,
                                    format,
                                    options,
                                });
                            }
                            Err(e) => {
                                let _ = tx.send(AppEvent::BackgroundError {
                                    generation: task_gen,
                                    message: format!(
                                        "Export failed: {}",
                                        crate::error_display::user_message_from_polars(&e)
                                    ),
                                });
                            }
                        }
                    });
                } else {
                    self.busy = false;
                }
                None
            }
            AppEvent::DoExportWrite(path, format, options) => {
                match self.export_df.take() {
                    Some(df) => {
                        let path = path.clone();
                        let format = *format;
                        let options = options.clone();
                        self.spawn_bg("Writing file...", move |task_gen, tx| {
                            let mut df = df;
                            let result =
                                Self::export_data_from_df(&mut df, &path, format, &options);
                            let _ = tx.send(AppEvent::BackgroundExportWritten {
                                generation: task_gen,
                                path: path.clone(),
                                result: result.map_err(|e| Self::format_export_error(&e, &path)),
                            });
                        });
                    }
                    _ => {
                        self.loading_state = LoadingState::Idle;
                        self.busy = false;
                    }
                }
                None
            }
            AppEvent::CopyTable { format, header } => {
                if let Some(state) = &self.data_table_state {
                    let lf = state.visible_lf();
                    let streaming = state.polars_streaming;
                    let (format, header) = (*format, *header);
                    self.spawn_bg("Collecting data for copy...", move |task_gen, tx| {
                        let ready = crate::statistics::collect_lazy(lf, streaming)
                            .map_err(|e| crate::error_display::user_message_from_polars(&e))
                            .and_then(|df| {
                                crate::clipboard::tabular_payload(&df, format, header)
                                    .map(|payload| (payload, df.height()))
                            });
                        let _ = tx.send(match ready {
                            Ok((payload, rows)) => AppEvent::BackgroundCopyReady {
                                generation: task_gen,
                                payload,
                                rows,
                                format,
                            },
                            Err(message) => AppEvent::BackgroundError {
                                generation: task_gen,
                                message: format!("Copy failed: {message}"),
                            },
                        });
                    });
                } else {
                    self.busy = false;
                }
                None
            }
            AppEvent::BackgroundCopyReady {
                generation,
                payload,
                rows,
                format,
            } => {
                if *generation == self.task_generation {
                    self.loading_state = LoadingState::Idle;
                    self.status_message = None;
                    self.busy = false;
                    let message = format!(
                        "Copied {} rows as {}",
                        copy_modal::thousands(*rows),
                        format.as_str()
                    );
                    self.finish_copy(payload.clone(), message);
                }
                None
            }
            AppEvent::DoLoadParquetMetadata => {
                let path = self.path.clone();
                if let Some(p) = &path
                    && let Some(meta) = read_parquet_metadata(p)
                {
                    self.parquet_metadata_cache = Some(meta);
                }
                self.busy = false;
                None
            }
            _ => None,
        }
    }

    /// Build the export from the prepared chart for the current selection. `Ok(None)`
    /// means that chart is still being prepared and the caller should wait for it.
    /// Exports what is visible (effective x + y); a blank title means no title.
    fn build_chart_export_job(&self, title: &str) -> Result<Option<ChartExportJob>> {
        if self.data_table_state.is_none() {
            return Err(color_eyre::eyre::eyre!("No data loaded"));
        }
        let chart_title = Some(title.trim())
            .filter(|t| !t.is_empty())
            .map(str::to_string);

        let request = match (
            ChartRequest::from_modal(&self.chart_modal),
            self.chart_modal.chart_kind,
        ) {
            (Some(ChartRequest::XRange { .. }), _) => {
                return Err(color_eyre::eyre::eyre!("No Y axis columns selected"));
            }
            (None, ChartKind::XY) => {
                return Err(color_eyre::eyre::eyre!("No X axis column selected"));
            }
            (None, ChartKind::Histogram) => {
                return Err(color_eyre::eyre::eyre!("No histogram column selected"));
            }
            (None, ChartKind::BoxPlot) => {
                return Err(color_eyre::eyre::eyre!("No box plot column selected"));
            }
            (None, ChartKind::Kde) => {
                return Err(color_eyre::eyre::eyre!("No KDE column selected"));
            }
            (None, ChartKind::Heatmap) => {
                return Err(color_eyre::eyre::eyre!("No heatmap columns selected"));
            }
            (Some(request), _) => request,
        };
        let prepared = match self.chart_cache.get(&request) {
            Some(Ok(prepared)) => prepared,
            // A selection known not to chart is never retried, so waiting for its data
            // would wait forever: fail the export now with the reason.
            Some(Err(message)) => return Err(color_eyre::eyre::eyre!("{}", message)),
            None => return Ok(None),
        };
        let no_points = || color_eyre::eyre::eyre!("No valid data points to export");

        let job = match prepared {
            ChartPrepared::XY(cache) => {
                let log_scale = self.chart_modal.log_scale;
                let points = if log_scale {
                    cache
                        .series_log
                        .clone()
                        .unwrap_or_else(|| log_series(&cache.series))
                } else {
                    cache.series.clone()
                };
                let series: Vec<ChartExportSeries> = points
                    .into_iter()
                    .zip(cache.y_columns.iter())
                    .filter(|(points, _)| !points.is_empty())
                    .map(|(points, name)| ChartExportSeries {
                        name: name.clone(),
                        points,
                    })
                    .collect();
                if series.is_empty() {
                    return Err(no_points());
                }

                let mut all_x_min = f64::INFINITY;
                let mut all_x_max = f64::NEG_INFINITY;
                let mut all_y_min = f64::INFINITY;
                let mut all_y_max = f64::NEG_INFINITY;
                for s in &series {
                    for &(x, y) in &s.points {
                        all_x_min = all_x_min.min(x);
                        all_x_max = all_x_max.max(x);
                        all_y_min = all_y_min.min(y);
                        all_y_max = all_y_max.max(y);
                    }
                }

                let chart_type = self.chart_modal.chart_type;
                let y_min_bounds = if chart_type == ChartType::Bar {
                    0.0_f64.min(all_y_min)
                } else if self.chart_modal.y_starts_at_zero {
                    0.0
                } else {
                    all_y_min
                };
                let y_max_bounds = if all_y_max > y_min_bounds {
                    all_y_max
                } else {
                    y_min_bounds + 1.0
                };
                let (x_min_bounds, x_max_bounds) = if all_x_max > all_x_min {
                    (all_x_min, all_x_max)
                } else {
                    (all_x_min - 0.5, all_x_min + 0.5)
                };

                let bounds = ChartExportBounds {
                    x_min: x_min_bounds,
                    x_max: x_max_bounds,
                    y_min: y_min_bounds,
                    y_max: y_max_bounds,
                    x_label: cache.x_column.clone(),
                    y_label: cache.y_columns.join(", "),
                    x_axis_kind: cache.x_axis_kind,
                    log_scale,
                    chart_title,
                };
                ChartExportJob::Series {
                    series,
                    chart_type,
                    bounds,
                }
            }
            ChartPrepared::Histogram(data) => {
                if data.bins.is_empty() {
                    return Err(no_points());
                }
                let points: Vec<(f64, f64)> =
                    data.bins.iter().map(|b| (b.center, b.count)).collect();
                let series = vec![ChartExportSeries {
                    name: data.column.clone(),
                    points,
                }];
                let x_max = if data.x_max > data.x_min {
                    data.x_max
                } else {
                    data.x_min + 1.0
                };
                let y_max = if data.max_count > 0.0 {
                    data.max_count
                } else {
                    1.0
                };
                let bounds = ChartExportBounds {
                    x_min: data.x_min,
                    x_max,
                    y_min: 0.0,
                    y_max,
                    x_label: data.column.clone(),
                    y_label: "Count".to_string(),
                    x_axis_kind: chart_data::XAxisTemporalKind::Numeric,
                    log_scale: false,
                    chart_title,
                };
                ChartExportJob::Series {
                    series,
                    chart_type: ChartType::Bar,
                    bounds,
                }
            }
            ChartPrepared::BoxPlot(data) => {
                if data.stats.is_empty() {
                    return Err(no_points());
                }
                let bounds = BoxPlotExportBounds {
                    y_min: data.y_min,
                    y_max: data.y_max,
                    x_labels: data.stats.iter().map(|s| s.name.clone()).collect(),
                    x_label: "Columns".to_string(),
                    y_label: "Value".to_string(),
                    chart_title,
                };
                ChartExportJob::BoxPlot {
                    data: data.clone(),
                    bounds,
                }
            }
            ChartPrepared::Kde(data) => {
                if data.series.is_empty() {
                    return Err(no_points());
                }
                let series: Vec<ChartExportSeries> = data
                    .series
                    .iter()
                    .map(|s| ChartExportSeries {
                        name: s.name.clone(),
                        points: s.points.clone(),
                    })
                    .collect();
                let bounds = ChartExportBounds {
                    x_min: data.x_min,
                    x_max: data.x_max,
                    y_min: 0.0,
                    y_max: data.y_max,
                    x_label: series
                        .iter()
                        .map(|s| s.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    y_label: "Density".to_string(),
                    x_axis_kind: chart_data::XAxisTemporalKind::Numeric,
                    log_scale: false,
                    chart_title,
                };
                ChartExportJob::Series {
                    series,
                    chart_type: ChartType::Line,
                    bounds,
                }
            }
            ChartPrepared::Heatmap(data) => {
                if data.counts.is_empty() || data.max_count <= 0.0 {
                    return Err(no_points());
                }
                let bounds = ChartExportBounds {
                    x_min: data.x_min,
                    x_max: data.x_max,
                    y_min: data.y_min,
                    y_max: data.y_max,
                    x_label: data.x_column.clone(),
                    y_label: data.y_column.clone(),
                    x_axis_kind: chart_data::XAxisTemporalKind::Numeric,
                    log_scale: false,
                    chart_title,
                };
                ChartExportJob::Heatmap {
                    data: data.clone(),
                    bounds,
                }
            }
            // Rejected above, since a single X column has nothing to export; never
            // `Ok(None)`, which would park the export waiting for data that is here.
            ChartPrepared::XRange(_) => {
                return Err(color_eyre::eyre::eyre!("No Y axis columns selected"));
            }
        };
        Ok(Some(job))
    }

    /// Write the chart from the prepared data off-thread, or park the export until that
    /// data is ready. `busy` was set by `ChartExport` and stays set until the export ends.
    fn start_chart_export(
        &mut self,
        path: PathBuf,
        format: ChartExportFormat,
        title: String,
        width: u32,
        height: u32,
    ) {
        match self.build_chart_export_job(&title) {
            Ok(Some(job)) => {
                self.chart_export_waiting = None;
                self.chart_export_generation = self.chart_export_generation.wrapping_add(1);
                let generation = self.chart_export_generation;
                self.chart_export_inflight = Some(generation);
                self.spawn_bg("Exporting chart...", move |_, tx| {
                    let result = job.write(&path, format, (width, height)).map_err(|e| {
                        crate::error_display::user_message_from_report(&e, Some(&path))
                    });
                    let _ = tx.send(AppEvent::BackgroundChartExportWritten {
                        generation,
                        path,
                        format,
                        result,
                    });
                });
            }
            // Still being prepared; `BackgroundChartReady` comes back here.
            Ok(None) => self.chart_export_waiting = Some((path, format, title, width, height)),
            Err(e) => {
                let message = crate::error_display::user_message_from_report(&e, Some(&path));
                self.finish_chart_export(&path, format, Err(message));
            }
        }
    }

    fn finish_chart_export(
        &mut self,
        path: &Path,
        format: ChartExportFormat,
        result: Result<(), String>,
    ) {
        self.chart_export_waiting = None;
        self.chart_export_inflight = None;
        self.loading_state = LoadingState::Idle;
        self.status_message = None;
        self.busy = false;
        match result {
            Ok(()) => {
                self.flash_note(format!("Chart exported to {}", path.display()));
                self.chart_export_modal.close();
            }
            Err(message) => {
                self.error_modal.show(message);
                self.chart_export_modal.reopen_with_path(path, format);
            }
        }
    }

    /// Bring the Sort & Filter sidebar in line with the state actually applied to the
    /// frame on screen: the real column order and hidden set, the applied sort, the
    /// active filters. Called on open, so an edit staged in the modal and then
    /// canceled dies with it rather than arriving pre-staged next time — and after a
    /// drill-down swap, where a sidebar still showing the grouped view's filters
    /// would re-send one against a List column.
    fn sync_sort_filter_modal(&mut self) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let filters = state.view_filters().to_vec();
        let sort_columns = state.view_sort_columns().to_vec();
        let sort_descending = state.view_sort_descending().to_vec();
        let headers: Vec<String> = state.schema.iter_names().map(|s| s.to_string()).collect();
        let order = state.headers();
        let locked = state.locked_columns_count();

        let modal = &mut self.sort_filter_modal;
        modal.filter.statements = filters;
        modal.filter.available_columns = order.clone();
        // The cursor starts on the add row; the editor never survives a resync.
        modal.filter.cursor = modal.filter.statements.len();
        modal.filter.editor = None;
        // A schema column the applied order leaves out is hidden; it lines up after
        // the visible ones, unlocked, exactly as toggling it back on would place it.
        let mut next_hidden_order = order.len();
        modal.sort.columns = headers
            .iter()
            .map(|name| {
                let position = order.iter().position(|c| c == name);
                let display_order = position.unwrap_or_else(|| {
                    next_hidden_order += 1;
                    next_hidden_order - 1
                });
                SortColumn {
                    name: name.clone(),
                    // 1-based: what toggling a column in the modal assigns and what
                    // the sidebar prints.
                    sort_order: sort_columns.iter().position(|c| c == name).map(|o| o + 1),
                    sort_descending: sort_columns
                        .iter()
                        .position(|c| c == name)
                        .and_then(|i| sort_descending.get(i).copied())
                        .unwrap_or(false),
                    display_order,
                    is_locked: position.is_some_and(|p| p < locked),
                    is_to_be_locked: false,
                    is_visible: position.is_some(),
                }
            })
            .collect();
        modal.sort.has_unapplied_changes = false;
    }

    /// Apply everything the sidebar stages — column order and locks, the sort with
    /// its per-column directions, the filters — and close it. Enter and Ctrl+Enter,
    /// from anywhere in the sidebar.
    fn apply_sort_filter(&mut self) -> Option<AppEvent> {
        // A row still under edit is committed, never silently dropped.
        if self.sort_filter_modal.filter.editor.is_some() {
            self.sort_filter_modal.filter.commit_editor();
        }
        let (columns, descending) = self.sort_filter_modal.sort.sorted_columns_and_directions();
        let column_order = self.sort_filter_modal.sort.get_column_order();
        let locked_count = self.sort_filter_modal.sort.get_locked_columns_count();
        let statements = self.sort_filter_modal.filter.statements.clone();
        for col in &mut self.sort_filter_modal.sort.columns {
            col.is_to_be_locked = false;
        }
        self.sort_filter_modal.sort.has_unapplied_changes = false;
        self.sort_filter_modal.close();
        self.input_mode = InputMode::Normal;
        let _ = self.send_event(AppEvent::ColumnOrder(column_order, locked_count));
        let _ = self.send_event(AppEvent::Filter(statements));
        Some(AppEvent::Sort(columns, descending))
    }

    /// Which of the Info panel's optional tabs the current dataset offers.
    fn info_tabs_on_offer(&self) -> (bool, bool) {
        let state = self.data_table_state.as_ref();
        let has_partitions = state
            .and_then(|s| s.partition_columns.as_ref())
            .map(|v| !v.is_empty())
            .unwrap_or(false);
        let has_notes = state.is_some_and(|s| s.has_notes());
        (has_partitions, has_notes)
    }

    /// The pipeline state a failed template application is rolled back to.
    fn snapshot_state(&self) -> Option<TemplateApplicationState> {
        self.data_table_state
            .as_ref()
            .map(|state| TemplateApplicationState {
                lf: state.lf.clone(),
                base_lf: state.base_lf_clone(),
                reshaped_lf: state.reshaped_lf_clone(),
                pivot: state.last_pivot_spec().cloned(),
                melt: state.last_melt_spec().cloned(),
                schema: state.schema.clone(),
                active_query: state.active_query.clone(),
                active_sql_query: state.get_active_sql_query().to_string(),
                active_fuzzy_query: state.get_active_fuzzy_query().to_string(),
                // The filters and sort applied to the frame being snapshotted (`lf`),
                // not the grouped view's when drilled.
                filters: state.view_filters().to_vec(),
                sort_columns: state.view_sort_columns().to_vec(),
                sort_descending: state.view_sort_descending().to_vec(),
                sort_ascending: state.view_sort_ascending(),
                column_order: state.get_column_order().to_vec(),
                locked_columns_count: state.locked_columns_count(),
                drift: state.drifts(),
                drift_groups: state.drift_groups(),
                notes: state.dataset_notes().to_vec(),
                notes_seen: state.notes_seen(),
            })
    }

    fn apply_template(&mut self, template: &Template) -> Result<()> {
        // Save state before applying template so we can restore on failure
        let saved_state = self.snapshot_state();
        let saved_active_template_id = self.active_template_id.clone();

        if let Some(state) = &mut self.data_table_state {
            state.error = None;

            // At most one of SQL or DSL query is stored per template; then fuzzy. Apply in that order.
            let sql_trimmed = template.settings.sql_query.as_deref().unwrap_or("").trim();
            let query_opt = template.settings.query.as_deref().filter(|s| !s.is_empty());
            let fuzzy_trimmed = template
                .settings
                .fuzzy_query
                .as_deref()
                .unwrap_or("")
                .trim();

            if !sql_trimmed.is_empty() {
                state.sql_query(template.settings.sql_query.clone().unwrap_or_default());
            } else if let Some(q) = query_opt {
                state.query(q.to_string());
            }
            if let Some(error) = state.error.clone() {
                if let Some(saved) = saved_state {
                    self.restore_state(saved);
                }
                self.active_template_id = saved_active_template_id;
                return Err(color_eyre::eyre::eyre!(
                    "{}",
                    crate::error_display::user_message_from_polars(&error)
                ));
            }

            if !fuzzy_trimmed.is_empty() {
                state.fuzzy_search(template.settings.fuzzy_query.clone().unwrap_or_default());
                if let Some(error) = state.error.clone() {
                    if let Some(saved) = saved_state {
                        self.restore_state(saved);
                    }
                    self.active_template_id = saved_active_template_id;
                    return Err(color_eyre::eyre::eyre!(
                        "{}",
                        crate::error_display::user_message_from_polars(&error)
                    ));
                }
            }

            // Apply filters
            if !template.settings.filters.is_empty() {
                state.filter(template.settings.filters.clone());
                // Check for errors after filter
                let error_opt = state.error.clone();
                if let Some(error) = error_opt {
                    // End the if let block to drop the borrow
                    if let Some(saved) = saved_state {
                        self.restore_state(saved);
                    }
                    self.active_template_id = saved_active_template_id;
                    return Err(color_eyre::eyre::eyre!("{}", error));
                }
            }

            // Apply sort
            if !template.settings.sort_columns.is_empty() {
                state.sort_by(
                    template.settings.sort_columns.clone(),
                    template.settings.sort_directions(),
                );
                // Check for errors after sort
                let error_opt = state.error.clone();
                if let Some(error) = error_opt {
                    // End the if let block to drop the borrow
                    if let Some(saved) = saved_state {
                        self.restore_state(saved);
                    }
                    self.active_template_id = saved_active_template_id;
                    return Err(color_eyre::eyre::eyre!("{}", error));
                }
            }

            // Apply pivot or melt (reshape) if present. Order: query → filters → sort → reshape → column_order.
            if let Some(ref spec) = template.settings.pivot {
                if let Err(e) = state.pivot(spec) {
                    if let Some(saved) = saved_state {
                        self.restore_state(saved);
                    }
                    self.active_template_id = saved_active_template_id;
                    return Err(color_eyre::eyre::eyre!(
                        "{}",
                        crate::error_display::user_message_from_report(&e, None)
                    ));
                }
            } else if let Some(ref spec) = template.settings.melt
                && let Err(e) = state.melt(spec)
            {
                if let Some(saved) = saved_state {
                    self.restore_state(saved);
                }
                self.active_template_id = saved_active_template_id;
                return Err(color_eyre::eyre::eyre!(
                    "{}",
                    crate::error_display::user_message_from_report(&e, None)
                ));
            }

            // Apply column order and locks
            if !template.settings.column_order.is_empty() {
                state.set_column_order(template.settings.column_order.clone());
                // Check for errors after set_column_order
                let error_opt = state.error.clone();
                if let Some(error) = error_opt {
                    // End the if let block to drop the borrow
                    if let Some(saved) = saved_state {
                        self.restore_state(saved);
                    }
                    self.active_template_id = saved_active_template_id;
                    return Err(color_eyre::eyre::eyre!("{}", error));
                }
                state.set_locked_columns(template.settings.locked_columns_count);
                // Check for errors after set_locked_columns
                let error_opt = state.error.clone();
                if let Some(error) = error_opt {
                    // End the if let block to drop the borrow
                    if let Some(saved) = saved_state {
                        self.restore_state(saved);
                    }
                    self.active_template_id = saved_active_template_id;
                    return Err(color_eyre::eyre::eyre!("{}", error));
                }
            }
        }

        // Update template usage statistics
        // Note: We need to clone and update the template, then save it
        // For now, we'll update the template manager's internal state
        // A more complete implementation would reload templates after saving
        if let Some(path) = &self.path {
            let mut updated_template = template.clone();
            updated_template.last_used = Some(std::time::SystemTime::now());
            updated_template.usage_count += 1;
            updated_template.last_matched_file = Some(path.clone());

            // Save updated template
            let _ = self.template_manager.save_template(&updated_template);
        }

        // Track active template
        self.active_template_id = Some(template.id.clone());

        Ok(())
    }

    /// Format export error messages to be more user-friendly using type-based handling.
    fn format_export_error(error: &color_eyre::eyre::Report, path: &Path) -> String {
        use std::io;

        for cause in error.chain() {
            if let Some(io_err) = cause.downcast_ref::<io::Error>() {
                let msg = crate::error_display::user_message_from_io(io_err, None);
                return format!("Cannot write to {}: {}", path.display(), msg);
            }
            if let Some(pe) = cause.downcast_ref::<polars::prelude::PolarsError>() {
                let msg = crate::error_display::user_message_from_polars(pe);
                return format!("Export failed: {}", msg);
            }
        }
        let error_str = error.to_string();
        let first_line = error_str.lines().next().unwrap_or("Unknown error").trim();
        format!("Export failed: {}", first_line)
    }

    /// Write an already-collected DataFrame to file. Used by two-phase export (DoExportWrite).
    /// Above this estimated size a table copy asks first: most paste targets
    /// choke long before it, and the clipboard holds the whole thing at once.
    const COPY_CONFIRM_BYTES: usize = 10 * 1024 * 1024;
    /// Above this a table copy is refused outright; a file is the medium for
    /// data this size, and export writes one without holding it all in text.
    const COPY_REFUSE_BYTES: usize = 200 * 1024 * 1024;

    /// Enter in the copy dialog: the synchronous scopes copy from the buffer
    /// and flash; the table scope guards on size, then collects off-thread.
    fn perform_copy(&mut self) -> Option<AppEvent> {
        use copy_modal::{CopyScope, thousands};
        /// What Enter decided, worked out under the table borrow and acted on
        /// after it: writing to the clipboard needs the whole app back.
        enum Planned {
            Copy(clipboard::Payload, String),
            Collect,
            /// None: the size is not known yet (the row count is still coming).
            Confirm(Option<usize>),
        }
        let format = self.copy_modal.format;
        let header = self.copy_modal.header();
        let scope = self.copy_modal.scope;
        let planned: Result<Planned, String> = match self.data_table_state.as_ref() {
            None => Err("Nothing to copy: no table is open".to_string()),
            Some(state) => match scope {
                CopyScope::Cell => {
                    let column = self.copy_modal.column.clone().unwrap_or_default();
                    match state.copy_cell_value(&column) {
                        Some(value) => {
                            let row = state.selected_display_row().unwrap_or(0);
                            Ok(Planned::Copy(
                                clipboard::Payload::text(value),
                                format!("Copied cell {column} of row {}", thousands(row)),
                            ))
                        }
                        None => Err("Nothing to copy: the current row is not buffered".to_string()),
                    }
                }
                CopyScope::Row => match state.copy_row_df() {
                    Some(df) => clipboard::tabular_payload(&df, format, header).map(|payload| {
                        let row = state.selected_display_row().unwrap_or(0);
                        Planned::Copy(
                            payload,
                            format!("Copied row {} as {}", thousands(row), format.as_str()),
                        )
                    }),
                    None => Err("Nothing to copy: the current row is not buffered".to_string()),
                },
                CopyScope::View => match state.copy_view_df() {
                    Some(df) => clipboard::tabular_payload(&df, format, header).map(|payload| {
                        Planned::Copy(
                            payload,
                            format!(
                                "Copied {} rows as {}",
                                thousands(df.height()),
                                format.as_str()
                            ),
                        )
                    }),
                    None => Err("Nothing to copy: no rows are on screen".to_string()),
                },
                CopyScope::Table => match state.estimated_total_bytes() {
                    Some(bytes) if bytes > Self::COPY_REFUSE_BYTES => Err(format!(
                        "The table is about {} — too much to hold on a clipboard. \
                         Export it to a file instead (e).",
                        Self::format_bytes(bytes as u64)
                    )),
                    Some(bytes) if bytes > Self::COPY_CONFIRM_BYTES => {
                        Ok(Planned::Confirm(Some(bytes)))
                    }
                    Some(_) => Ok(Planned::Collect),
                    // The row count has not landed yet, so the size is anyone's
                    // guess: ask before collecting an unknown amount.
                    None => Ok(Planned::Confirm(None)),
                },
            },
        };
        self.copy_modal.close();
        self.input_mode = InputMode::Normal;
        match planned {
            Ok(Planned::Copy(payload, message)) => {
                self.finish_copy(payload, message);
                None
            }
            Ok(Planned::Collect) => Some(AppEvent::CopyTable { format, header }),
            Ok(Planned::Confirm(bytes)) => {
                self.pending_copy = Some((format, header));
                self.confirmation_modal.show(match bytes {
                    Some(bytes) => format!(
                        "This copies about {} to the clipboard.\n\nCopy the whole table?",
                        Self::format_bytes(bytes as u64)
                    ),
                    None => "The table's size is not known yet — the row count is \
                             still being read.\n\nCopy the whole table anyway?"
                        .to_string(),
                });
                None
            }
            Err(message) => {
                self.error_modal.show(message);
                None
            }
        }
    }

    /// Hand a payload to the clipboard destination, building the destination
    /// at the first copy, and flash or raise the error modal — a copy that
    /// silently did nothing would be worse than one that failed out loud.
    fn finish_copy(&mut self, payload: clipboard::Payload, message: String) {
        if self.clipboard.is_none() {
            let choice = clipboard::BackendChoice::parse(&self.app_config.clipboard.backend)
                .unwrap_or_default();
            let limit = self.app_config.clipboard.osc52_limit_kb * 1024;
            match clipboard::destination(choice, limit) {
                Ok(destination) => self.clipboard = Some(destination),
                Err(e) => {
                    self.error_modal.show(e);
                    return;
                }
            }
        }
        let destination = self.clipboard.as_mut().expect("destination just built");
        match destination.write(&payload) {
            Ok(()) => self.flash_note(message),
            Err(e) => self.error_modal.show(e),
        }
    }

    /// Replace the clipboard destination, so tests can watch what a copy sends
    /// without a display server or a terminal in the loop.
    pub fn set_clipboard_destination(&mut self, destination: Box<dyn clipboard::Destination>) {
        self.clipboard = Some(destination);
    }

    fn export_data_from_df(
        df: &mut DataFrame,
        path: &Path,
        format: ExportFormat,
        options: &ExportOptions,
    ) -> Result<()> {
        use polars::prelude::*;
        use std::fs::File;
        use std::io::{BufWriter, Write};

        match format {
            ExportFormat::Csv => {
                use polars::prelude::CsvWriter;
                if let Some(compression) = options.csv_compression {
                    // Write to compressed file
                    let file = File::create(path)?;
                    let writer: Box<dyn Write> = match compression {
                        CompressionFormat::Gzip => Box::new(flate2::write::GzEncoder::new(
                            file,
                            flate2::Compression::default(),
                        )),
                        CompressionFormat::Zstd => {
                            Box::new(zstd::Encoder::new(file, 0)?.auto_finish())
                        }
                        CompressionFormat::Bzip2 => Box::new(bzip2::write::BzEncoder::new(
                            file,
                            bzip2::Compression::default(),
                        )),
                        CompressionFormat::Xz => {
                            Box::new(xz2::write::XzEncoder::new(
                                file, 6, // compression level
                            ))
                        }
                    };
                    CsvWriter::new(writer)
                        .with_separator(options.csv_delimiter)
                        .include_header(options.csv_include_header)
                        .finish(df)?;
                } else {
                    // Write uncompressed
                    let file = File::create(path)?;
                    CsvWriter::new(file)
                        .with_separator(options.csv_delimiter)
                        .include_header(options.csv_include_header)
                        .finish(df)?;
                }
            }
            ExportFormat::Parquet => {
                use polars::prelude::ParquetWriter;
                let file = File::create(path)?;
                let mut writer = BufWriter::new(file);
                ParquetWriter::new(&mut writer).finish(df)?;
            }
            ExportFormat::Json => {
                use polars::prelude::JsonWriter;
                if let Some(compression) = options.json_compression {
                    // Write to compressed file
                    let file = File::create(path)?;
                    let writer: Box<dyn Write> = match compression {
                        CompressionFormat::Gzip => Box::new(flate2::write::GzEncoder::new(
                            file,
                            flate2::Compression::default(),
                        )),
                        CompressionFormat::Zstd => {
                            Box::new(zstd::Encoder::new(file, 0)?.auto_finish())
                        }
                        CompressionFormat::Bzip2 => Box::new(bzip2::write::BzEncoder::new(
                            file,
                            bzip2::Compression::default(),
                        )),
                        CompressionFormat::Xz => {
                            Box::new(xz2::write::XzEncoder::new(
                                file, 6, // compression level
                            ))
                        }
                    };
                    JsonWriter::new(writer)
                        .with_json_format(JsonFormat::Json)
                        .finish(df)?;
                } else {
                    // Write uncompressed
                    let file = File::create(path)?;
                    JsonWriter::new(file)
                        .with_json_format(JsonFormat::Json)
                        .finish(df)?;
                }
            }
            ExportFormat::Ndjson => {
                use polars::prelude::{JsonFormat, JsonWriter};
                if let Some(compression) = options.ndjson_compression {
                    // Write to compressed file
                    let file = File::create(path)?;
                    let writer: Box<dyn Write> = match compression {
                        CompressionFormat::Gzip => Box::new(flate2::write::GzEncoder::new(
                            file,
                            flate2::Compression::default(),
                        )),
                        CompressionFormat::Zstd => {
                            Box::new(zstd::Encoder::new(file, 0)?.auto_finish())
                        }
                        CompressionFormat::Bzip2 => Box::new(bzip2::write::BzEncoder::new(
                            file,
                            bzip2::Compression::default(),
                        )),
                        CompressionFormat::Xz => {
                            Box::new(xz2::write::XzEncoder::new(
                                file, 6, // compression level
                            ))
                        }
                    };
                    JsonWriter::new(writer)
                        .with_json_format(JsonFormat::JsonLines)
                        .finish(df)?;
                } else {
                    // Write uncompressed
                    let file = File::create(path)?;
                    JsonWriter::new(file)
                        .with_json_format(JsonFormat::JsonLines)
                        .finish(df)?;
                }
            }
            ExportFormat::Ipc => {
                use polars::prelude::IpcWriter;
                let file = File::create(path)?;
                let mut writer = BufWriter::new(file);
                IpcWriter::new(&mut writer).finish(df)?;
            }
            ExportFormat::Avro => {
                use polars::io::avro::AvroWriter;
                let file = File::create(path)?;
                let mut writer = BufWriter::new(file);
                AvroWriter::new(&mut writer).finish(df)?;
            }
        }

        Ok(())
    }

    #[allow(dead_code)] // Used only when not using two-phase export; kept for tests/single-shot use
    fn export_data(
        state: &DataTableState,
        path: &Path,
        format: ExportFormat,
        options: &ExportOptions,
    ) -> Result<()> {
        let mut df = crate::statistics::collect_lazy(state.visible_lf(), state.polars_streaming)?;
        Self::export_data_from_df(&mut df, path, format, options)
    }

    fn restore_state(&mut self, saved: TemplateApplicationState) {
        if let Some(state) = &mut self.data_table_state {
            // Clone saved lf and schema so we can restore them after applying methods
            let saved_lf = saved.lf.clone();
            let saved_schema = saved.schema.clone();

            // Restore lf and schema directly (these are public fields)
            // This preserves the exact LazyFrame state from before template application
            state.lf = saved.lf;
            state.set_base_lf(saved.base_lf);
            // Before the filter and sort below, not after. They rebuild the notes about
            // what the view leaves out, and they can only do that while the state still
            // knows the rows stand for rows of a file — which the failed template's own
            // query turned off. Put back afterwards instead and the restored frame goes
            // on leaving rows out with nothing on screen saying why.
            state.restore_drift(saved.drift, saved.drift_groups, saved.notes);
            // Without this a template that pivoted and then failed would leave the
            // pivot as the root SQL runs against while the view shows none.
            state.restore_reshape(saved.reshaped_lf, saved.pivot, saved.melt);
            state.schema = saved.schema;
            state.active_query = saved.active_query;
            state.active_sql_query = saved.active_sql_query;
            state.active_fuzzy_query = saved.active_fuzzy_query;
            state.error = None;
            // The projection first: these two decide what the frame is read as, and a
            // filter or sort applied while the template's column order is still in
            // place collects against a column that may not be there. That used to
            // error, and each step here was guarded on the one before, so the rest of
            // the rollback was abandoned — leaving the template's sort in the sidebar
            // over the user's own frame.
            state.set_column_order(saved.column_order.clone());
            state.set_locked_columns(saved.locked_columns_count);
            // Unguarded, for the same reason: a rollback that stops half way leaves a
            // state neither the template's nor the user's. Whatever these make of the
            // frame is thrown away two lines below; what they are here for is the view
            // state they set on the way, and every one of them has to be the user's.
            state.filter(saved.filters.clone());
            if saved.sort_columns.is_empty() {
                // Nothing sorted: `sort_ascending` alone carries a reversed natural order.
                state.sort(Vec::new(), saved.sort_ascending);
            } else {
                state.sort_by(saved.sort_columns.clone(), saved.sort_descending.clone());
            }
            // Restore the exact saved lf and schema (in case filter/sort modified them)
            state.lf = saved_lf;
            state.schema = saved_schema;
            // The count is left as the rebuild above measured it. `base_lf` follows
            // every pipeline root, so what `sort` rebuilt from it is the same frame
            // this one is, and invalidating here only threw the footer count away and
            // made the next `collect` count a remote dataset over again, on this
            // thread, for a number it already had.
            //
            // Any error those steps raised was about a frame that is no longer here.
            state.error = None;
            if saved.notes_seen {
                state.mark_notes_seen();
            }
            state.collect();
        }
    }

    pub fn create_template_from_current_state(
        &mut self,
        name: String,
        description: Option<String>,
        match_criteria: template::MatchCriteria,
    ) -> Result<template::Template> {
        let settings = if let Some(state) = &self.data_table_state {
            let (query, sql_query, fuzzy_query) = active_query_settings(
                state.get_active_query(),
                state.get_active_sql_query(),
                state.get_active_fuzzy_query(),
            );
            template::TemplateSettings {
                query,
                sql_query,
                fuzzy_query,
                filters: state.get_filters().to_vec(),
                sort_columns: state.get_sort_columns().to_vec(),
                sort_descending: state.get_sort_descending().to_vec(),
                sort_ascending: state.get_sort_ascending(),
                column_order: state.get_column_order().to_vec(),
                locked_columns_count: state.locked_columns_count(),
                pivot: state.last_pivot_spec().cloned(),
                melt: state.last_melt_spec().cloned(),
            }
        } else {
            template::TemplateSettings {
                query: None,
                sql_query: None,
                fuzzy_query: None,
                filters: Vec::new(),
                sort_columns: Vec::new(),
                sort_descending: Vec::new(),
                sort_ascending: true,
                column_order: Vec::new(),
                locked_columns_count: 0,
                pivot: None,
                melt: None,
            }
        };

        self.template_manager
            .create_template(name, description, match_criteria, settings)
    }

    /// The query prompt's mode while it is open.
    pub fn query_prompt_mode(&self) -> Option<QueryMode> {
        (self.input_mode == InputMode::Editing && self.input_type == Some(InputType::Search))
            .then_some(self.query_mode)
    }

    /// The mode `/` opens on: the active query's own, so editing never
    /// reinterprets it in another language; otherwise the configured default.
    fn opening_query_mode(&self) -> QueryMode {
        let active = self.data_table_state.as_ref().and_then(|state| {
            if !state.get_active_sql_query().trim().is_empty() {
                Some(QueryMode::Sql)
            } else if !state.get_active_fuzzy_query().trim().is_empty() {
                Some(QueryMode::Search)
            } else if !state.get_active_query().trim().is_empty() {
                Some(QueryMode::QStyle)
            } else {
                None
            }
        });
        active
            .unwrap_or(self.app_config.query.default_mode)
            .resolve()
    }

    /// Switch the prompt's mode. Each mode keeps its own text; an error from the
    /// last run belongs to the mode that ran it.
    fn set_query_mode(&mut self, mode: QueryMode) {
        self.query_mode = mode.resolve();
        if let Some(state) = &mut self.data_table_state {
            state.error = None;
        }
        self.sync_query_focus();
    }

    /// Only the current mode's input carries the cursor, and only while the
    /// input, not the tab bar, has focus.
    fn sync_query_focus(&mut self) {
        let input = self.query_focus == QueryFocus::Input;
        let mode = self.query_mode;
        self.sql_input.set_focused(input && mode == QueryMode::Sql);
        self.fuzzy_input
            .set_focused(input && mode == QueryMode::Search);
        self.query_input
            .set_focused(input && mode == QueryMode::QStyle);
    }

    /// Esc from anywhere in the prompt: nothing runs and nothing typed survives.
    fn close_query_prompt(&mut self) {
        self.query_input.clear();
        self.sql_input.clear();
        self.fuzzy_input.clear();
        self.query_input.set_focused(false);
        self.sql_input.set_focused(false);
        self.fuzzy_input.set_focused(false);
        self.input_mode = InputMode::Normal;
        self.input_type = None;
        if let Some(state) = &mut self.data_table_state {
            state.error = None;
            state.suppress_error_display = false;
        }
    }

    /// Rows the active search matched, once the count is settled, while the
    /// Search input still holds the words that ran. A sidebar filter on top
    /// makes the row count something else, so then there is none to show.
    pub(crate) fn search_match_count(&self) -> Option<usize> {
        let state = self.data_table_state.as_ref()?;
        let ran = state.get_active_fuzzy_query();
        (!ran.trim().is_empty()
            && self.fuzzy_input.value() == ran
            && state.get_filters().is_empty()
            && !state.is_drilled_down()
            && state.is_num_rows_valid()
            && !self.row_count_pending())
        .then_some(state.num_rows)
    }

    fn get_help_info(&self) -> (String, String) {
        let (title, content) = match self.input_mode {
            InputMode::Normal => ("Table Help", help_strings::main_view()),
            InputMode::Editing => match self.input_type {
                Some(InputType::Search) => ("Query Help", help_strings::query()),
                _ => ("Go to Line", help_strings::go_to_line()),
            },
            InputMode::SortFilter => ("Sort & Filter Help", help_strings::sort_filter()),
            InputMode::PivotMelt => ("Pivot & Melt Help", help_strings::pivot_melt()),
            InputMode::Export => ("Export Help", help_strings::export()),
            InputMode::Copy => ("Copy Help", help_strings::copy()),
            InputMode::Info => ("Info Panel Help", help_strings::info_panel()),
            InputMode::Chart => ("Chart Help", help_strings::chart()),
            InputMode::Home => ("Home Help", help_strings::home()),
        };
        (title.to_string(), content.to_string())
    }
}

impl Widget for &mut App {
    fn render(self, area: Rect, buf: &mut Buffer) {
        self.begin_frame();
        self.debug.num_frames += 1;
        if self.debug.enabled {
            self.debug.show_help_at_render = self.show_help;
        }

        use crate::render::context::RenderContext;
        use crate::render::layout::app_layout;
        use crate::render::main_view::MainViewContent;

        let ctx = RenderContext::from_theme_and_config(
            &self.theme,
            self.table_cell_padding,
            self.column_colors,
            self.number_format.clone(),
        )
        .with_dtype_row(self.dtype_row);

        let main_view_content = MainViewContent::current(self);

        Clear.render(area, buf);
        let background_color = self.color("background");
        Block::default()
            .style(Style::default().bg(background_color))
            .render(area, buf);

        let app_layout = app_layout(area, self.debug.enabled);
        let main_area = app_layout.main_view;
        Clear.render(main_area, buf);

        crate::render::main_view_render::render_main_view(area, main_area, buf, self, &ctx);

        // Status messages are shown inline in the control bar (no overlay popups).

        if self.confirmation_modal.active {
            crate::render::overlays::render_confirmation_modal(
                area,
                buf,
                &mut self.confirmation_modal,
                &ctx,
            );
        }
        if self.error_modal.active {
            crate::render::overlays::render_error_modal(area, buf, &mut self.error_modal, &ctx);
        }
        if self.show_help
            || (self.template_modal.active && self.template_modal.show_help)
            || (self.analysis_modal.active && self.analysis_modal.show_help)
        {
            let (title, text): (String, String) =
                if self.analysis_modal.active && self.analysis_modal.show_help {
                    crate::render::analysis_view::help_title_and_text(&self.analysis_modal)
                } else if self.template_modal.active {
                    ("Views Help".to_string(), help_strings::views().to_string())
                } else {
                    let (t, txt) = self.get_help_info();
                    (t.to_string(), txt.to_string())
                };
            crate::render::overlays::render_help_overlay(
                area,
                buf,
                &title,
                &text,
                &mut self.help_scroll,
                &ctx,
            );
        }

        let row_count = self.data_table_state.as_ref().map(|s| s.num_rows);
        // The spinner follows the glyph set, so it cannot disagree with the rest of
        // the chrome about whether the terminal is doing UTF-8.
        let use_unicode_throbber = crate::glyphs::active_is_unicode();
        let mut controls = Controls::from_context(row_count.unwrap_or(0), &ctx)
            .with_unicode_throbber(use_unicode_throbber);

        // Derive status message from loading_state or explicit status_message.
        let status_msg = match &self.loading_state {
            LoadingState::Loading {
                current_phase,
                progress_percent,
                ..
            } => {
                let current_phase = self.loading_phase(current_phase);
                // The percentage is a constant per phase, which was harmless beside a
                // phase name and is not beside a real fraction: 1,203 of 6,541 is 18%,
                // and "(40%)" next to it reads as that count's progress. The same
                // number the phase was built from, so a pass that ends mid-frame
                // cannot leave the count showing with the percentage back beside it.
                let counting = self.footers_this_frame.is_some();
                if *progress_percent > 0 && !counting {
                    Some(format!("{}... ({}%)", current_phase, progress_percent))
                } else {
                    Some(format!("{}...", current_phase))
                }
            }
            LoadingState::Exporting {
                current_phase,
                progress_percent,
                file_path,
            } => {
                let filename = file_path.file_name().and_then(|f| f.to_str()).unwrap_or("");
                if *progress_percent > 0 {
                    Some(format!(
                        "{}... ({}%)  {}",
                        current_phase, progress_percent, filename
                    ))
                } else {
                    Some(format!("{}...  {}", current_phase, filename))
                }
            }
            LoadingState::Idle => {
                if self.fetch_too_young_to_mention() {
                    None
                } else if self.busy {
                    self.status_message.clone()
                } else if let Some((read, total)) = self
                    .footers_this_frame
                    .filter(|_| self.dataset_is_still_reading_its_footers())
                {
                    // The dataset opened from two footers and is still learning the
                    // rest. Said quietly, because nothing is wrong and nothing is
                    // blocked: the columns it finds will join what is already here.
                    Some(format!(
                        "Reading footers: {} of {}...",
                        crate::numfmt::group_chrome(read),
                        crate::numfmt::group_chrome(total)
                    ))
                } else if self.chart_preparing() {
                    Some("Preparing chart...".to_string())
                } else if main_view_content == MainViewContent::Datatable {
                    // Whatever is on the line, busy or not. An End waiting on a remote
                    // count parks without setting `busy` — keys go on working meanwhile,
                    // which is the point of parking — so both the message explaining the
                    // wait and the one saying the count failed were written here and
                    // painted by nothing.
                    //
                    // Only at the table, because that is what these messages are about.
                    // A parked End survives Ctrl+O, and the home screen has a caption and
                    // a row count of its own: shown there it would replace every key chip
                    // on the bar with a sentence about a dataset the user has left.
                    //
                    // Not every message needs this branch. The one `jump_key` puts up
                    // while a footer pass is running is superseded by the footers line
                    // above, which says the same thing with numbers.
                    self.status_message.clone()
                } else {
                    None
                }
            }
        };
        let status_msg = status_msg.map(|msg| {
            if self.input_dropped && self.busy {
                format!("{msg}  input dropped while busy")
            } else {
                msg
            }
        });
        controls = controls.with_status_message(status_msg);
        controls = controls.with_flash(self.flash.as_ref().map(|f| f.message.clone()));
        controls = controls.with_reshaped(self.data_table_state.as_ref().and_then(|s| {
            if s.last_pivot_spec().is_some() {
                Some("pivoted")
            } else if s.last_melt_spec().is_some() {
                Some("melted")
            } else {
                None
            }
        }));
        controls = controls.with_not_the_table(
            self.data_table_state
                .as_ref()
                .and_then(|s| s.not_the_table()),
        );
        controls = controls.with_notes_pending(
            self.app_config.display.notes_accent
                && self
                    .data_table_state
                    .as_ref()
                    .is_some_and(|s| s.notes_unseen()),
        );

        match crate::render::main_view::control_bar_spec(self, main_view_content) {
            crate::render::main_view::ControlBarSpec::Datatable {
                dimmed,
                query_active,
                q_pops,
            } => {
                controls = controls
                    .with_dimmed(dimmed)
                    .with_query_active(query_active)
                    .with_q_pops(q_pops);
            }
            crate::render::main_view::ControlBarSpec::Custom(pairs) => {
                controls = controls.with_custom_controls(pairs);
            }
        }

        // The trailing figure belongs to whatever view is showing. On the home screen
        // that is how many datasets are listed, not the table's row count.
        if main_view_content == MainViewContent::Home {
            // Only things that can actually be opened. A directory is somewhere to
            // look, not a dataset, and counting it makes the figure a lie — and so
            // does counting a directory nothing has looked into yet, which in a fresh
            // listing is every directory in it.
            // Past the cap on RECENT: a dataset the `more` row stands for is listed,
            // and the header above it counts it.
            let datasets = self
                .home
                .listed()
                .iter()
                .filter(|r| {
                    // The door is not among these: its kind is the directory's, so it
                    // would count as a dataset and be the same dataset as the directory —
                    // the figure this comment calls a lie, counted twice. It is a
                    // `Row::Door` and not an entry, so nothing here has to exclude it.
                    matches!(r, home::Row::Entry { entry, .. } if entry.kind.is_known_dataset())
                })
                .count();
            // State, not actions: how many datasets are listed and what order they
            // are in. The Tab key that changes it lives with the other keys.
            let in_recents = self
                .home
                .selected_section()
                .and_then(|i| self.home.sections.get(i))
                .map(|s| s.grouped_by_place)
                .unwrap_or(false);
            let order = self.home.sort.label_in(in_recents);
            let waiting = self.home.listing_in_flight || self.home.awaiting_listing().is_some();
            let dot = crate::glyphs::get().middot;
            let caption = if waiting && datasets == 0 {
                "Looking...".to_string()
            } else if datasets == 1 {
                format!("by {order}  {dot}  1 dataset")
            } else {
                format!("by {order}  {dot}  {datasets} datasets")
            };
            controls = controls.with_caption(Some(caption));
        }

        // Chart preparation spins the throbber without setting `busy`, so the chart
        // sidebar keeps taking keys while the data is computed.
        controls = controls.with_busy(self.busy || self.chart_preparing(), self.throbber_frame);
        // Reflect the row-count's determinacy in the control bar:
        //  - in flight   -> spinner (still being computed)
        //  - failed       -> "?" (computation gave up; don't show a misleading partial total)
        //  - otherwise    -> the number
        let count_pending = self.row_count_pending();
        let count_unknown = !count_pending
            && self.data_table_state.as_ref().is_some_and(|s| {
                !s.is_num_rows_valid() && self.len_count_failed == Some(s.len_generation())
            });
        controls = controls
            .with_row_count_pending(count_pending)
            .with_row_count_unknown(count_unknown)
            // "417 of 1,000" under a filter or query. Only a total something already
            // resolved: never a reason for the chrome to read data.
            .with_total_row_count(
                self.data_table_state
                    .as_ref()
                    .and_then(|s| s.total_rows_when_subset()),
            );
        controls.render(app_layout.control_bar, buf);
        if let Some(debug_area) = app_layout.debug {
            self.debug.render(debug_area, buf);
        }

        // Last line of defence, and deliberately the last statement here.
        //
        // Everything above draws untrusted text: cell values, column names,
        // filenames, parser messages. ratatui strips control characters in
        // `Buffer::set_stringn` but not in `Span`/`Line` rendering, which is
        // what these widgets use, and the crossterm backend then prints each
        // cell symbol unfiltered. Without this sweep a cell containing
        // `\x1b]52;c;...\x07` writes to the user's clipboard.
        //
        // Doing it here rather than at each of the ~200 `Span` construction
        // sites means a new widget cannot forget to. See `crate::sanitize`.
        crate::sanitize::sanitize_buffer(buf);
    }
}

impl App {
    /// The view a caller that asked for one gets back when the app exits
    /// (`datui.view(..., capture=True)`): the active table's committed frame with
    /// datui's internal columns dropped. `None` when no dataset is open. Text still
    /// sitting in an editor was never applied, so it is not here either.
    ///
    /// Refused when the frame would scan a temporary file, because those are removed
    /// on exit and a plan over deleted paths fails later and worse: a remote download
    /// (`http_temp_path`) or a decompressed archive. The in-TUI export (`e`) writes
    /// real rows and is the way out for those datasets.
    pub fn capture_view(&self) -> Result<Option<LazyFrame>> {
        let Some(state) = &self.data_table_state else {
            return Ok(None);
        };
        #[cfg(any(feature = "http", feature = "cloud"))]
        if self.http_temp_path.is_some() {
            return Err(color_eyre::eyre::eyre!(
                "cannot return this view: the data was downloaded to a temporary file \
                 that is removed when datui exits. Export it from inside datui (press \
                 e) instead."
            ));
        }
        if state.scans_a_temp_file() {
            return Err(color_eyre::eyre::eyre!(
                "cannot return this view: the compressed file was decompressed into a \
                 temporary file that is removed when datui exits. Export it from \
                 inside datui (press e) instead."
            ));
        }
        Ok(Some(state.visible_lf()))
    }
}

impl Drop for App {
    fn drop(&mut self) {
        // Opening a remote file downloads it to a temp file so Polars can scan
        // it lazily. Opening a *different* file removes the previous one, which
        // is what `AppEvent::Open` does, but quitting removed nothing: the last
        // dataset someone viewed stayed in the temp directory until something
        // else cleared it. The file is mode 0600, so this is disk hygiene
        // rather than exposure, but the contents are the user's data and they
        // did not ask for a copy to be left behind.
        //
        // Drop rather than the end of `run`, because it is the one place that
        // covers every exit: a normal quit, an error return, an unwind from a
        // panic, and the Python binding calling `run` again in the same
        // process.
        #[cfg(any(feature = "http", feature = "cloud"))]
        if let Some(path) = self.http_temp_path.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// A source as a home-screen row, with the last run's buckets when they still apply.
#[cfg(feature = "cloud")]
fn home_cloud_source(
    source: &crate::cloud_sources::Source,
    cached: Option<&crate::cache::CloudListing>,
    listing: bool,
) -> home::CloudSource {
    let mut details: Vec<(String, String)> = vec![
        ("source".to_string(), source.id.clone()),
        (
            "api".to_string(),
            match source.kind {
                crate::cloud_browse::ProviderKind::S3 => "s3",
                crate::cloud_browse::ProviderKind::Gcs => "gcs",
                crate::cloud_browse::ProviderKind::Azure => "azure",
            }
            .to_string(),
        ),
    ];
    if let Some(endpoint) = &source.s3.endpoint {
        details.push(("endpoint".to_string(), endpoint.clone()));
    }
    if let Some(region) = &source.s3.region {
        details.push(("region".to_string(), region.clone()));
    }
    if let Some(project) = &source.project {
        details.push(("project".to_string(), project.clone()));
    }
    if let Some(profile) = &source.profile {
        details.push(("profile".to_string(), profile.clone()));
    }
    if let Some(configuration) = &source.gcloud {
        details.push(("configuration".to_string(), configuration.clone()));
    }
    if source.s3.virtual_hosted.is_some() {
        let style = if source.s3.virtual_hosted_style() {
            "virtual-hosted"
        } else {
            "path-style"
        };
        details.push(("addressing".to_string(), style.to_string()));
    }
    details.push(("login".to_string(), source.origin.clone()));

    let note = [source.detail(), Some(source.origin.clone())]
        .into_iter()
        .flatten()
        .filter(|n| !n.is_empty())
        .collect::<Vec<_>>()
        .join(&format!(" {} ", crate::glyphs::get().middot));
    let mut names: Vec<String> = cached.map(|c| c.buckets.clone()).unwrap_or_default();
    for bucket in &source.buckets {
        if !names.contains(bucket) {
            names.push(bucket.clone());
        }
    }
    let status = match &source.problem {
        Some(problem) => home::CloudStatus::Failed {
            short: if problem.starts_with("not signed in") {
                "not signed in"
            } else if problem.starts_with("unsupported login") {
                "unsupported login"
            } else {
                "not configured"
            }
            .to_string(),
            detail: problem.clone(),
        },
        None if cached.is_some() => home::CloudStatus::Listed,
        None if listing => home::CloudStatus::Listing,
        None => home::CloudStatus::Unlisted,
    };
    home::CloudSource {
        id: source.id.clone(),
        label: source.label.clone(),
        api: match source.kind {
            crate::cloud_browse::ProviderKind::S3 => "s3",
            crate::cloud_browse::ProviderKind::Gcs => "gcs",
            crate::cloud_browse::ProviderKind::Azure => "azure",
        }
        .to_string(),
        note,
        buckets: names
            .iter()
            .map(|b| PathBuf::from(source.bucket_url(b)))
            .collect(),
        refreshing: listing && cached.is_some() && source.problem.is_none(),
        // A source that failed before any request has nothing to ask.
        asked: listing || source.problem.is_some(),
        listed_at: cached
            .map(|c| std::time::UNIX_EPOCH + std::time::Duration::from_secs(c.listed_at)),
        status,
        details,
        place_details: Default::default(),
    }
}

/// A listing error as a word for the row and the full message for the details pane.
#[cfg(feature = "cloud")]
fn summarize_cloud_failure(error: &str) -> (String, String) {
    let lower = error.to_lowercase();
    // A missing tool is already as short as it gets: `needs the AWS CLI`.
    if let Some(start) = lower.find("needs ") {
        return (error[start..].to_string(), error.to_string());
    }
    let short = if lower.contains("403")
        || lower.contains("forbidden")
        || lower.contains("accessdenied")
        || lower.contains("access denied")
    {
        "403"
    } else if lower.contains("401")
        || lower.contains("unauthorized")
        || lower.contains("credential")
        || lower.contains("invalidaccesskeyid")
        || lower.contains("expired")
        || lower.contains("sso")
        || lower.contains("az login")
    {
        "not logged in"
    } else if lower.contains("unsupported login") {
        "unsupported login"
    } else if lower.contains("no gcp project") {
        "no project"
    } else if lower.contains("is not set") {
        "not configured"
    } else if lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("connection")
        || lower.contains("dns")
        || lower.contains("resolve")
    {
        "unavailable"
    } else {
        "error"
    };
    (short.to_string(), error.to_string())
}

/// Run a future on the app's runtime from a thread outside it, and wait for the answer.
///
/// Every background thread that needs the network goes through this rather than
/// `Handle::block_on`. That polls the future on the calling thread, and quitting shuts
/// the runtime down without waiting for those threads: the next timer or socket an
/// in-flight request touches then panics with "A Tokio 1.x context was found, but it is
/// being shutdown", across the terminal the user just got back. A task spawned onto the
/// runtime is dropped by the shutdown instead of polled, so the wait ends with `None`
/// and the abandoned request goes quietly.
#[cfg(feature = "cloud")]
fn wait_on_runtime<F>(runtime: &tokio::runtime::Handle, future: F) -> Option<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    runtime.spawn(async move {
        let _ = tx.send(future.await);
    });
    rx.recv().ok()
}

/// How the event loop ended, for `run_impl` to turn into its return value once the
/// terminal is restored.
enum RunEnd {
    Quit,
    Crash(String),
}

/// Folds one channel drain into the loop: records whether the app changed, or says
/// how the loop should end.
fn finish_drain(drained: event_pump::Drained, updated: &mut bool) -> Option<RunEnd> {
    match drained {
        event_pump::Drained::Continue { updated: changed } => {
            *updated |= changed;
            None
        }
        event_pump::Drained::Exit => Some(RunEnd::Quit),
        event_pump::Drained::Crash(msg) => Some(RunEnd::Crash(msg)),
    }
}

/// Restore the terminal, then turn how the loop ended into what `run_impl` returns.
/// The capture is taken after the screen is handed back, so a refused capture still
/// leaves the terminal usable.
fn conclude(end: RunEnd, app: &App, capture: bool) -> Result<Option<LazyFrame>> {
    restore_terminal();
    match end {
        RunEnd::Quit if capture => app.capture_view(),
        RunEnd::Quit => Ok(None),
        RunEnd::Crash(msg) => Err(color_eyre::eyre::eyre!(msg)),
    }
}

/// Undo `run`'s terminal setup: pop the keyboard flags (a no-op where they were
/// never pushed; a terminal that ignored the push ignores the pop too), then hand
/// back the screen.
fn restore_terminal() {
    let _ = crossterm::execute!(
        std::io::stdout(),
        crossterm::event::PopKeyboardEnhancementFlags
    );
    ratatui::restore();
}

/// Run the TUI with either file paths or an existing LazyFrame. Single event loop
/// used by the CLI and the Python binding.
pub fn run(input: RunInput, config: Option<AppConfig>) -> Result<()> {
    run_impl(input, config, false).map(|_| ())
}

/// As `run`, but a normal quit hands back the active table's final view for the
/// caller to keep working with (the Python binding's `capture=True`). `None` when no
/// dataset was open at quit. See `App::capture_view` for what is refused and why.
pub fn run_captured(input: RunInput, config: Option<AppConfig>) -> Result<Option<LazyFrame>> {
    run_impl(input, config, true)
}

fn run_impl(
    input: RunInput,
    config: Option<AppConfig>,
    capture: bool,
) -> Result<Option<LazyFrame>> {
    use event_pump::EventPump;
    use std::io::Write;
    use std::sync::{Mutex, Once, mpsc};

    let config = match config {
        Some(c) => c,
        None => AppConfig::load(APP_NAME)?,
    };

    let opts = match &input {
        RunInput::Paths(_, o) => o.clone(),
        RunInput::LazyFrame(_, o) => o.clone(),
    };
    // The home screen has no `OpenOptions` of its own, so the CLI and environment
    // S3 overrides are folded into the config here, once, for discovery, listing and
    // opens started from a listed bucket.
    let mut config = config;
    // Variables from `[cloud] env_files` first, so everything below sees them.
    if let Ok(dir) = std::env::current_dir() {
        for note in crate::cloud_env::load(&config.cloud, &dir) {
            eprintln!("datui: {note}");
        }
    }
    config.cloud = opts.effective_cloud(&config.cloud);

    let theme = Theme::from_config(&config.theme)
        .or_else(|e| Theme::from_config(&AppConfig::default().theme).map_err(|_| e))?;

    // Install color_eyre at most once per process (e.g. first datui.view() in Python).
    // Subsequent run() calls skip install and reuse the result; no error-message detection.
    static COLOR_EYRE_INIT: Once = Once::new();
    static INSTALL_RESULT: Mutex<Option<Result<(), color_eyre::Report>>> = Mutex::new(None);
    COLOR_EYRE_INIT.call_once(|| {
        *INSTALL_RESULT.lock().unwrap_or_else(|e| e.into_inner()) = Some(color_eyre::install());
    });
    if let Some(Err(e)) = INSTALL_RESULT
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
    {
        return Err(color_eyre::eyre::eyre!(e.to_string()));
    }
    // No paths is no longer an error: it means "start at home". Validation below
    // still applies to any paths that were given.
    if let RunInput::Paths(ref paths, _) = input {
        for path in paths {
            let is_glob = path.to_string_lossy().contains('*');
            if !source::is_remote_url(path) && !is_glob && !path.exists() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("File not found: {}", path.display()),
                )
                .into());
            }
        }
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|e| color_eyre::eyre::eyre!("Failed to create tokio runtime: {}", e))?;

    // Background work (e.g. the row-count `len()` over a huge or remote dataset) runs on
    // the runtime's blocking pool. Dropping the runtime normally *joins* those threads, so
    // quitting would hang until an in-flight count finished — minutes for a 474 GB hive
    // set. Shut the runtime down in the background instead: exit is immediate and the
    // abandoned read-only task dies with the process. This guard covers every return path
    // (Exit, Crash, `?`-propagated errors, channel disconnect).
    struct RtGuard(Option<tokio::runtime::Runtime>);
    impl Drop for RtGuard {
        fn drop(&mut self) {
            if let Some(rt) = self.0.take() {
                rt.shutdown_background();
            }
        }
    }
    let rt_guard = RtGuard(Some(rt));
    let rt_handle = rt_guard
        .0
        .as_ref()
        .expect("runtime present")
        .handle()
        .clone();

    // Choose the glyph alphabet before the first frame: on a terminal that is not
    // doing UTF-8, box-drawing characters render as replacement boxes and make the
    // UI harder to read rather than prettier.
    glyphs::init_with_overrides(config.display.unicode, &config.glyphs.overrides);

    let mut terminal = ratatui::try_init().map_err(|e| {
        color_eyre::eyre::eyre!(
            "datui requires an interactive terminal (TTY). No terminal detected: {}. \
             There is no TTY inside a Jupyter notebook or when output is piped or \
             redirected; run from a terminal with stdout connected to it.",
            e
        )
    })?;
    // Without the kitty keyboard protocol, Ctrl+Enter is byte-identical to Enter and
    // the Ctrl never reaches the app. Disambiguation alone fixes that — plain Enter,
    // Tab and Backspace keep their legacy encodings — and the terminal keeps a
    // separate flag stack for the alternate screen, so leaving it on exit or panic
    // restores the shell's keyboard either way.
    if matches!(
        crossterm::terminal::supports_keyboard_enhancement(),
        Ok(true)
    ) {
        let _ = crossterm::execute!(
            std::io::stdout(),
            crossterm::event::PushKeyboardEnhancementFlags(
                crossterm::event::KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES
            )
        );
    }
    let (tx, rx) = mpsc::channel::<AppEvent>();
    let mut app = App::new_with_config(tx.clone(), rt_handle, theme, config.clone());
    app.startup_template = opts.template.clone();
    if opts.debug {
        app.enable_debug();
    }

    // Send initial event and show the first frame immediately.
    let mut starting_at_home = false;
    match input {
        // No paths: open the home screen instead of loading anything.
        RunInput::Paths(paths, _) if paths.is_empty() => {
            app.enter_home();
            starting_at_home = true;
        }
        RunInput::Paths(paths, opts) => {
            // A directory named here is read the way `Enter` reads its row, which may be
            // by opening the home screen on it rather than by loading anything. The
            // looking is an event, not a call: it is sent here and carried out after
            // the first frame, so a directory that takes seconds to look at says which
            // directory it is looking at while it does.
            match app.open_the_path_named_on_the_command_line(paths, opts) {
                Some(event) => {
                    // The first frame is drawn before any event is handled, so what it
                    // says has to be set here — the handler's own phase lands a frame
                    // later, and "Scanning input" on a directory nothing has read yet is
                    // the wrong word for the wait the user is actually in.
                    match &event {
                        AppEvent::LookThenOpenDirectory(dir, _) => {
                            app.set_loading_phase(App::LOOKING_AT_A_DIRECTORY, 5);
                            app.name_what_is_loading(dir.clone());
                        }
                        _ => app.set_loading_phase("Scanning input", 10),
                    }
                    tx.send(event)?;
                }
                None => starting_at_home = true,
            }
        }
        RunInput::LazyFrame(lf, opts) => {
            app.set_loading_phase("Scanning input", 10);
            tx.send(AppEvent::OpenLazyFrame(lf, opts))?;
        }
    }
    app.busy = !starting_at_home;
    let mut pump = EventPump::new(app, tx, rx);
    terminal.draw(|frame| frame.render_widget(&mut pump.app, frame.area()))?;
    let _ = std::io::stdout().flush();

    // Main event loop: replay one held key, poll for input, drain the channel, redraw.
    loop {
        let mut updated = pump.replay_one()?;
        // A replayed key may have queued a follow-up (a Search, an Export); handle it
        // before the terminal is read so a key typed now cannot overtake it.
        if let Some(end) = finish_drain(pump.drain()?, &mut updated) {
            return conclude(end, &pump.app, capture);
        }
        let app = &pump.app;

        // Poll with a shorter timeout when busy so the throbber animates (~30fps).
        // 33ms is plenty for a spinner and halves redraw load vs. 60fps. Held keys
        // waiting on an idle app replay one per iteration, so then there is no wait.
        let spinning = app.something_is_spinning();
        let poll_ms = if pump.replaying() {
            0
        } else if spinning {
            33
        } else {
            config.performance.event_poll_interval_ms
        };

        if crossterm::event::poll(std::time::Duration::from_millis(poll_ms))? {
            match crossterm::event::read()? {
                crossterm::event::Event::Key(key) if key.is_press() => {
                    updated |= pump.terminal_key(key)?;
                }
                crossterm::event::Event::Resize(cols, rows) => {
                    pump.send(AppEvent::Resize(cols, rows))?;
                }
                _ => {}
            }
        }

        if let Some(end) = finish_drain(pump.drain()?, &mut updated) {
            return conclude(end, &pump.app, capture);
        }
        let app = &mut pump.app;

        // Animate throbber when busy or while the background row count is still resolving
        // (that count doesn't set `busy` but drives the row-count spinner).
        if spinning {
            app.throbber_frame = app.throbber_frame.wrapping_add(1);
            updated = true;
        }

        // A completion flash times out on its own; the idle poll interval is the
        // clock, so no extra wake-up machinery is needed.
        updated |= app.tick_flash();

        app.request_what_the_frame_needs();

        if updated {
            terminal.draw(|frame| frame.render_widget(&mut *app, frame.area()))?;
            // After render, check if visible_rows changed and trigger async buffer re-collect.
            if let Some(state) = &mut app.data_table_state
                && state.needs_recollect
            {
                state.needs_recollect = false;
                app.spawn_async_collect(App::LOADING_BUFFER);
            }
        }
    }
}

#[cfg(all(test, feature = "cloud"))]
mod cloud_csv_prefix_tests {
    use super::*;

    /// The details pane says why a bucket directory's `(all files)` row reads nothing
    /// by the rule Enter applies, so it is never shown for one Enter would read.
    #[test]
    fn the_door_says_why_only_when_enter_would_read_nothing() {
        let door = |formats: &[(&str, usize)], not_read| {
            let mut entry = discover::Entry::directory(Path::new("s3://b/dir/"));
            entry.opens_whole_directory = true;
            entry.holds = discover::Holds {
                formats: formats.iter().map(|(f, n)| (f.to_string(), *n)).collect(),
                not_read,
                ..Default::default()
            };
            entry
        };
        let why = App::why_a_door_reads_nothing(&door(&[("tsv", 2)], 0));
        assert!(why.is_some_and(|why| why.contains("2 tsv")));
        assert!(App::why_a_door_reads_nothing(&door(&[], 3)).is_some());
        assert_eq!(App::why_a_door_reads_nothing(&door(&[("csv", 3)], 0)), None);
        assert_eq!(
            App::why_a_door_reads_nothing(&door(&[("parquet", 3)], 0)),
            None
        );
        let mut hive = door(&[], 3);
        hive.kind = discover::EntryKind::Hive;
        assert_eq!(App::why_a_door_reads_nothing(&hive), None);
    }

    /// A CSV prefix in a bucket is read with the flags the user gave, not Polars'
    /// defaults. Driven through a local glob, which is the same reader with the object
    /// store swapped for the filesystem.
    #[test]
    fn a_csv_prefix_is_read_with_the_users_flags() {
        let dir = tempfile::tempdir().unwrap();
        let preamble = "exported by x\nid;name\n1;NA\n2;bob\n3;FOOTER\n";
        std::fs::write(dir.path().join("a.csv"), preamble).unwrap();
        let glob = format!("{}/*.csv", dir.path().display());
        let options = OpenOptions {
            delimiter: Some(b';'),
            skip_lines: Some(1),
            skip_tail_rows: Some(1),
            null_values: Some(vec!["NA".into()]),
            ..OpenOptions::default()
        };
        let df = App::scan_cloud_prefix(
            &glob,
            CloudOptions::default(),
            FileFormat::Csv,
            true,
            &options,
        )
        .expect("a CSV reader")
        .unwrap()
        .collect()
        .unwrap();
        let names: Vec<_> = df
            .get_column_names()
            .iter()
            .map(|n| n.to_string())
            .collect();
        assert_eq!(names, ["id", "name"]);
        assert_eq!(df.height(), 2);
        assert_eq!(df.column("name").unwrap().null_count(), 1);

        // Global and per-column null values together read the columns from the
        // prefix itself.
        let options = OpenOptions {
            null_values: Some(vec!["NA".into(), "name=bob".into()]),
            ..options
        };
        let df = App::scan_cloud_prefix(
            &glob,
            CloudOptions::default(),
            FileFormat::Csv,
            true,
            &options,
        )
        .expect("a CSV reader")
        .unwrap()
        .collect()
        .unwrap();
        assert_eq!(df.column("name").unwrap().null_count(), 1);
        assert_eq!(df.column("id").unwrap().null_count(), 0);
    }

    /// A CSV prefix keeps timestamps as text rather than risk a read that fails on
    /// them: Polars' date inference takes `2024-01-01 10:00:00 UTC`, as BigQuery
    /// exports it, for a datetime and then cannot parse it.
    #[test]
    fn a_csv_prefix_keeps_timestamps_as_text() {
        let dir = tempfile::tempdir().unwrap();
        let csv = "z,bq\n2013-01-01T10:00:00Z,2024-01-01 10:00:00 UTC\n\
                   2013-01-01T11:00:00.5Z,2024-01-02 11:30:15 UTC\n";
        std::fs::write(dir.path().join("a.csv"), csv).unwrap();
        let glob = format!("{}/*.csv", dir.path().display());
        let options = OpenOptions {
            parse_strings: Some(ParseStringsTarget::All),
            ..OpenOptions::default()
        };
        let df = App::scan_cloud_prefix(
            &glob,
            CloudOptions::default(),
            FileFormat::Csv,
            true,
            &options,
        )
        .expect("a CSV reader")
        .unwrap()
        .collect()
        .expect("the read succeeds");
        assert_eq!(df.height(), 2);
        assert_eq!(df.column("z").unwrap().dtype(), &DataType::String);
        assert_eq!(df.column("bq").unwrap().dtype(), &DataType::String);
    }

    /// With one source, `local`, whose endpoint refuses every connection: a browse
    /// lists where it lands, and nothing here leaves the machine.
    fn new_app() -> App {
        let (tx, _rx) = std::sync::mpsc::channel();
        let mut config = AppConfig::default();
        config.cloud.connections = vec![crate::config::CloudConnectionConfig {
            name: "local".to_string(),
            kind: Some("s3".to_string()),
            endpoint_url: Some("http://127.0.0.1:9".to_string()),
            ..Default::default()
        }];
        App::new_with_config(
            tx,
            crate::tests::test_runtime(),
            Theme {
                colors: std::collections::HashMap::new(),
            },
            config,
        )
    }

    /// A cloud directory on the command line is listed before it is opened; a file, a
    /// glob or `--format` is opened as named.
    #[test]
    fn a_cloud_directory_named_on_the_command_line_is_looked_at_first() {
        let named = |path: &str, options: OpenOptions| {
            new_app().open_the_path_named_on_the_command_line(vec![PathBuf::from(path)], options)
        };
        for directory in ["s3://local@b/census/data/", "s3://local@b/census"] {
            assert!(matches!(
                named(directory, OpenOptions::default()),
                Some(AppEvent::LookThenOpenDirectory(..))
            ));
        }
        let csv = OpenOptions {
            format: Some(FileFormat::Csv),
            ..OpenOptions::default()
        };
        for (path, options) in [
            ("s3://local@b/census/data/test.csv", OpenOptions::default()),
            ("s3://local@b/census/**/*.csv", OpenOptions::default()),
            ("s3://local@b/census/data/", csv),
        ] {
            assert!(
                matches!(named(path, options), Some(AppEvent::Open(..))),
                "{path}"
            );
        }
    }

    /// What the listing found decides it, as it does for the `(all files)` row.
    #[test]
    fn a_cloud_directory_opens_as_its_listing_says() {
        use discover::{EntryKind, Holds};
        let dir = PathBuf::from("s3://local@b/census/data");
        let holding = |formats: &[(&str, usize)], directories| Holds {
            formats: formats.iter().map(|(f, n)| (f.to_string(), *n)).collect(),
            directories,
            ..Holds::default()
        };

        // CSV files: read as CSV, as a prefix.
        let mut app = new_app();
        let csv = holding(&[("csv", 3)], 0);
        let Some(AppEvent::Open(paths, options)) = app.open_the_directory_looked_at(
            dir.clone(),
            EntryKind::Directory,
            Some(&csv),
            OpenOptions::default(),
        ) else {
            panic!("a directory of CSV opens");
        };
        assert_eq!(paths, [PathBuf::from("s3://local@b/census/data/")]);
        assert_eq!(options.format, Some(FileFormat::Csv));

        // Only directories: browsed.
        let mut app = new_app();
        let subdirectories = holding(&[], 1);
        assert!(
            app.open_the_directory_looked_at(
                dir.clone(),
                EntryKind::Directory,
                Some(&subdirectories),
                OpenOptions::default(),
            )
            .is_none()
        );
        assert_eq!(app.input_mode, InputMode::Home);
        assert_eq!(app.home.browsing.as_deref(), Some(dir.as_path()));

        // A listing that was refused: opened as named, so the error is the store's.
        let mut app = new_app();
        assert!(matches!(
            app.open_the_directory_looked_at(
                dir.clone(),
                EntryKind::Unknown,
                None,
                OpenOptions::default(),
            ),
            Some(AppEvent::Open(paths, _)) if paths == [dir.clone()]
        ));
    }

    /// A folder marker listed beside the files is not read as one of them. Polars
    /// refused the whole prefix over it: "different file extensions".
    #[test]
    fn a_folder_marker_in_a_csv_prefix_is_not_read() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.csv"), "id\n1\n").unwrap();
        std::fs::write(dir.path().join("b.csv"), "id\n2\n").unwrap();
        std::fs::write(dir.path().join("data"), "placeholder").unwrap();
        let prefix = format!("{}/", dir.path().display());
        let df = App::scan_cloud_prefix(
            &prefix,
            CloudOptions::default(),
            FileFormat::Csv,
            true,
            &OpenOptions::default(),
        )
        .expect("a CSV reader")
        .unwrap()
        .collect()
        .unwrap();
        assert_eq!(df.height(), 2);
    }
}

#[cfg(test)]
mod feedback_ladder_tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use polars::prelude::IntoLazy;
    use std::sync::mpsc;

    fn key(app: &mut App, code: KeyCode) -> Option<AppEvent> {
        app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)))
    }

    /// Enter on an incomplete pivot form re-accents the gap line instead of
    /// raising a modal; the next key dims it again.
    #[test]
    fn an_incomplete_pivot_apply_stays_inline() {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        app.pivot_melt_modal.active = true;
        app.input_mode = InputMode::PivotMelt;
        key(&mut app, KeyCode::Enter);
        assert!(!app.error_modal.active, "validation is not a failure");
        assert!(app.pivot_melt_modal.attention, "the gap line is lit");
        key(&mut app, KeyCode::Tab);
        assert!(!app.pivot_melt_modal.attention, "an edit dims it again");
    }

    /// `s` in the views list with an untouched table refuses on the list's own
    /// status line; the next key clears it.
    #[test]
    fn the_views_save_refusal_stays_on_the_surface() {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        let df = polars::df!("a" => [1i64, 2]).unwrap();
        app.data_table_state = Some(
            crate::widgets::datatable::DataTableState::new(df.lazy(), None, None, None, None, true)
                .unwrap(),
        );
        app.template_modal.active = true;
        key(&mut app, KeyCode::Char('s'));
        assert!(!app.error_modal.active, "a refusal is not a failure");
        assert!(
            app.template_modal
                .status
                .as_deref()
                .unwrap_or("")
                .starts_with("Nothing to save"),
            "the refusal is on the list's status line"
        );
        key(&mut app, KeyCode::Down);
        assert!(
            app.template_modal.status.is_none(),
            "the next key clears it"
        );
    }
}
